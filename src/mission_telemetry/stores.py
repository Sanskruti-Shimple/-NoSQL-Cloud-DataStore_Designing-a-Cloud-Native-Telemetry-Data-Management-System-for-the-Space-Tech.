from __future__ import annotations

import asyncio
import json
import logging
import ssl
from datetime import UTC, date, datetime, timedelta
from typing import TYPE_CHECKING, Any

from neo4j import AsyncDriver, AsyncGraphDatabase
from pymongo import AsyncMongoClient
from redis.asyncio import Redis

from mission_telemetry.models import Alert, TelemetryPacket
from mission_telemetry.settings import Settings

if TYPE_CHECKING:
    from cassandra.cluster import Session

logger = logging.getLogger(__name__)


class CassandraStore:
    def __init__(
        self,
        hosts: list[str],
        port: int,
        keyspace: str,
        username: str | None = None,
        password: str | None = None,
        ca_file: str | None = None,
        use_tls: bool = False,
        bootstrap_schema: bool = True,
    ) -> None:
        self._hosts = hosts
        self._port = port
        self._keyspace = keyspace
        self._username = username
        self._password = password
        self._ca_file = ca_file
        self._use_tls = use_tls
        self._bootstrap_schema = bootstrap_schema
        self._cluster: Any | None = None
        self._session: Session | None = None

    @property
    def session(self) -> Session:
        if self._session is None:
            raise RuntimeError("Cassandra store is not connected")
        return self._session

    async def connect(self) -> None:
        def initialize() -> tuple[Any, Session]:
            from cassandra.auth import PlainTextAuthProvider
            from cassandra.cluster import Cluster

            ssl_context = (
                ssl.create_default_context(cafile=self._ca_file)
                if self._use_tls or self._ca_file
                else None
            )
            auth_provider = (
                PlainTextAuthProvider(username=self._username, password=self._password)
                if self._username and self._password
                else None
            )
            cluster = Cluster(
                self._hosts,
                port=self._port,
                ssl_context=ssl_context,
                auth_provider=auth_provider,
            )
            if self._use_tls:
                session = cluster.connect(self._keyspace)
            else:
                session = cluster.connect()
                if self._bootstrap_schema:
                    session.execute(
                        f"""
                        CREATE KEYSPACE IF NOT EXISTS {self._keyspace}
                        WITH replication = {{'class': 'SimpleStrategy', 'replication_factor': 1}}
                        """
                    )
                session.set_keyspace(self._keyspace)
            if self._bootstrap_schema:
                session.execute(
                    """
                    CREATE TABLE IF NOT EXISTS telemetry_by_sensor_day (
                        satellite_id text,
                        sensor_id text,
                        day_bucket date,
                        observed_at timestamp,
                        event_id uuid,
                        metric text,
                        value double,
                        unit text,
                        module_id text,
                        sequence bigint,
                        PRIMARY KEY ((satellite_id, sensor_id, day_bucket), observed_at, event_id)
                    ) WITH CLUSTERING ORDER BY (observed_at DESC, event_id ASC)
                    """
                )
            return cluster, session

        self._cluster, self._session = await asyncio.to_thread(initialize)

    async def write(self, packet: TelemetryPacket) -> None:
        await asyncio.to_thread(
            self.session.execute,
            """
            INSERT INTO telemetry_by_sensor_day
            (satellite_id, sensor_id, day_bucket, observed_at, event_id,
             metric, value, unit, module_id, sequence)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (
                packet.satellite_id,
                packet.sensor_id,
                packet.observed_at.date(),
                packet.observed_at,
                packet.event_id,
                packet.metric,
                packet.value,
                packet.unit,
                packet.module_id,
                packet.sequence,
            ),
        )

    async def history(
        self, satellite_id: str, sensor_id: str, start: datetime, end: datetime
    ) -> list[dict[str, Any]]:
        async def read_day(bucket: date) -> list[dict[str, Any]]:
            rows = await asyncio.to_thread(
                self.session.execute,
                """
                SELECT event_id, observed_at, metric, value, unit, module_id, sequence
                FROM telemetry_by_sensor_day
                WHERE satellite_id = %s AND sensor_id = %s AND day_bucket = %s
                AND observed_at >= %s AND observed_at <= %s
                """,
                (satellite_id, sensor_id, bucket, start, end),
            )
            return [
                {
                    "event_id": str(row.event_id),
                    "observed_at": (
                        row.observed_at.replace(tzinfo=UTC)
                        if row.observed_at.tzinfo is None
                        else row.observed_at.astimezone(UTC)
                    ).isoformat(),
                    "metric": row.metric,
                    "value": row.value,
                    "unit": row.unit,
                    "module_id": row.module_id,
                    "sequence": row.sequence,
                }
                for row in rows
            ]

        day_count = (end.date() - start.date()).days
        results = await asyncio.gather(
            *(read_day(start.date() + timedelta(days=offset)) for offset in range(day_count + 1))
        )
        return sorted(
            (item for day_rows in results for item in day_rows),
            key=lambda item: item["observed_at"],
            reverse=True,
        )

    async def close(self) -> None:
        if self._session is not None:
            await asyncio.to_thread(self._session.shutdown)
            self._session = None
        if self._cluster is not None:
            await asyncio.to_thread(self._cluster.shutdown)
            self._cluster = None


class Platform:
    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self.mongo: AsyncMongoClient[dict[str, Any]] | None = None
        self.redis: Redis | None = None
        self._demo_packets: dict[str, dict[str, Any]] = {}
        self._demo_health: dict[tuple[str, str], dict[str, Any]] = {}
        self._demo_alerts: dict[str, str] = {}
        self._demo_rate_limits: dict[str, tuple[int, int]] = {}
        self._demo_graph: dict[str, set[tuple[str, str, str, str, str]]] = {}
        self.cassandra = CassandraStore(
            [host.strip() for host in settings.cassandra_hosts.split(",") if host.strip()],
            settings.cassandra_port,
            settings.cassandra_keyspace,
            settings.cassandra_username,
            (
                settings.cassandra_password.get_secret_value()
                if settings.cassandra_password
                else None
            ),
            settings.cassandra_ca_file,
            settings.app_env.lower() == "production",
            settings.cassandra_schema_bootstrap,
        )
        self.neo4j: AsyncDriver | None = None

    def _redis(self) -> Redis:
        if self.redis is None:
            raise RuntimeError("Redis store is not connected")
        return self.redis

    def _mongo_collection(self) -> Any:
        if self.mongo is None:
            raise RuntimeError("MongoDB store is not connected")
        return self.mongo[self.settings.mongodb_database]["telemetry_packets"]

    def _neo4j(self) -> AsyncDriver:
        if self.neo4j is None:
            raise RuntimeError("Neo4j store is not connected")
        return self.neo4j

    async def connect(self) -> None:
        if self.settings.demo_mode:
            logger.warning(
                "Demo mode uses in-memory NoSQL projections; data is lost when the app restarts"
            )
            return
        try:
            self.mongo = AsyncMongoClient(self.settings.mongodb_uri, serverSelectionTimeoutMS=5000)
            await self.mongo.admin.command("ping")
            await self._mongo_collection().create_index(
                [("satellite_id", 1), ("sensor_id", 1), ("observed_at", -1)],
                name="sensor_latest",
            )
            self.redis = Redis.from_url(self.settings.redis_url, decode_responses=True)
            await self.redis.ping()
            await self.cassandra.connect()
            self.neo4j = AsyncGraphDatabase.driver(
                self.settings.neo4j_uri,
                auth=(
                    self.settings.neo4j_username,
                    self.settings.neo4j_password.get_secret_value(),
                ),
            )
            await self._neo4j().verify_connectivity()
            if self.settings.neo4j_schema_bootstrap:
                async with self._neo4j().session() as session:
                    await session.run(
                        "CREATE CONSTRAINT satellite_id IF NOT EXISTS "
                        "FOR (n:Satellite) REQUIRE n.id IS UNIQUE"
                    )
                    await session.run(
                        "CREATE CONSTRAINT module_id IF NOT EXISTS "
                        "FOR (n:Module) REQUIRE (n.satellite_id, n.id) IS UNIQUE"
                    )
                    await session.run(
                        "CREATE CONSTRAINT sensor_id IF NOT EXISTS "
                        "FOR (n:Sensor) REQUIRE (n.satellite_id, n.id) IS UNIQUE"
                    )
        except Exception:
            await self.close()
            raise

    async def persist(self, packet: TelemetryPacket) -> None:
        document = packet.as_document()
        if self.settings.demo_mode:
            self._demo_packets[str(packet.event_id)] = document
            self._demo_health[(packet.satellite_id, packet.sensor_id)] = document
            self._demo_graph.setdefault(packet.satellite_id, set()).update(
                {
                    ("Satellite", packet.satellite_id, "Module", packet.module_id, "CONTAINS"),
                    ("Module", packet.module_id, "Sensor", packet.sensor_id, "HAS_SENSOR"),
                }
            )
            return
        await self._mongo_collection().replace_one(
            {"_id": str(packet.event_id)}, document, upsert=True
        )
        await self.cassandra.write(packet)

        async with self._neo4j().session() as session:
            await session.run(
                """
                MERGE (satellite:Satellite {id: $satellite_id})
                MERGE (module:Module {satellite_id: $satellite_id, id: $module_id})
                MERGE (sensor:Sensor {satellite_id: $satellite_id, id: $sensor_id})
                MERGE (satellite)-[:CONTAINS]->(module)
                MERGE (module)-[:HAS_SENSOR]->(sensor)
                """,
                satellite_id=packet.satellite_id,
                module_id=packet.module_id,
                sensor_id=packet.sensor_id,
            )

        redis = self._redis()
        await redis.hset(
            f"health:{packet.satellite_id}",
            packet.sensor_id,
            json.dumps(document, separators=(",", ":")),
        )
        await redis.expire(f"health:{packet.satellite_id}", 86400)

    async def latest(self, satellite_id: str, sensor_id: str) -> dict[str, Any] | None:
        if self.settings.demo_mode:
            return self._demo_health.get((satellite_id, sensor_id))
        result = await self._mongo_collection().find_one(
            {"satellite_id": satellite_id, "sensor_id": sensor_id},
            sort=[("observed_at", -1)],
            projection={"_id": 0},
        )
        return result

    async def health_snapshot(self, satellite_id: str) -> list[dict[str, Any]]:
        if self.settings.demo_mode:
            return [
                reading
                for (stored_satellite_id, _), reading in self._demo_health.items()
                if stored_satellite_id == satellite_id
            ]
        values = await self._redis().hgetall(f"health:{satellite_id}")
        return [json.loads(value) for value in values.values()]

    async def history(
        self, satellite_id: str, sensor_id: str, start: datetime, end: datetime
    ) -> list[dict[str, Any]]:
        if self.settings.demo_mode:
            return sorted(
                (
                    {
                        "event_id": document["event_id"],
                        "observed_at": document["observed_at"],
                        "metric": document["metric"],
                        "value": document["value"],
                        "unit": document["unit"],
                        "module_id": document["module_id"],
                        "sequence": document["sequence"],
                    }
                    for document in self._demo_packets.values()
                    if document["satellite_id"] == satellite_id
                    and document["sensor_id"] == sensor_id
                    and start <= datetime.fromisoformat(document["observed_at"]) <= end
                ),
                key=lambda item: item["observed_at"],
                reverse=True,
            )
        return await self.cassandra.history(satellite_id, sensor_id, start, end)

    async def record_alert(self, alert: Alert) -> bool:
        if self.settings.demo_mode:
            payload = alert.model_dump_json()
            alert_id = str(alert.alert_id)
            if alert_id in self._demo_alerts:
                return False
            self._demo_alerts[alert_id] = payload
            return True
        redis = self._redis()
        payload = alert.model_dump_json()
        stored = await redis.set(f"alert:event:{alert.alert_id}", payload, ex=604800, nx=True)
        if stored:
            await redis.lpush("mission:alerts", payload)
            await redis.ltrim("mission:alerts", 0, 999)
            await redis.expire("mission:alerts", 2_592_000)
        return bool(stored)

    async def alerts(self, limit: int) -> list[dict[str, Any]]:
        if self.settings.demo_mode:
            return [
                json.loads(payload)
                for payload in list(self._demo_alerts.values())[-limit:][::-1]
            ]
        payloads = await self._redis().lrange("mission:alerts", 0, limit - 1)
        return [json.loads(payload) for payload in payloads]

    async def dependencies(self, satellite_id: str) -> dict[str, list[dict[str, Any]]]:
        if self.settings.demo_mode:
            nodes: dict[tuple[str, str], dict[str, str]] = {
                ("Satellite", satellite_id): {"id": satellite_id, "type": "Satellite"}
            }
            relationships: list[dict[str, str]] = []
            for source_type, source_id, target_type, target_id, relationship_type in sorted(
                self._demo_graph.get(satellite_id, set())
            ):
                nodes[(source_type, source_id)] = {"id": source_id, "type": source_type}
                nodes[(target_type, target_id)] = {"id": target_id, "type": target_type}
                relationships.append(
                    {
                        "source": source_id,
                        "target": target_id,
                        "type": relationship_type,
                    }
                )
            return {"nodes": list(nodes.values()), "relationships": relationships}
        async with self._neo4j().session() as session:
            result = await session.run(
                """
                MATCH path=(satellite:Satellite {id: $satellite_id})
                  -[:CONTAINS|HAS_SENSOR*1..2]->(node)
                WITH nodes(path) AS path_nodes, relationships(path) AS path_relationships
                RETURN [n IN path_nodes | {id: n.id, type: labels(n)[0]}] AS nodes,
                       [r IN path_relationships | {
                           source: startNode(r).id,
                           target: endNode(r).id,
                           type: type(r)
                       }] AS relationships
                """,
                satellite_id=satellite_id,
            )
            nodes: dict[tuple[str, str], dict[str, str]] = {}
            relationships: dict[tuple[str, str, str], dict[str, str]] = {}
            async for record in result:
                for node in record["nodes"]:
                    nodes[(node["type"], node["id"])] = node
                for relationship in record["relationships"]:
                    key = (
                        relationship["source"],
                        relationship["target"],
                        relationship["type"],
                    )
                    relationships[key] = relationship
            return {
                "nodes": list(nodes.values()),
                "relationships": list(relationships.values()),
            }

    async def rate_limit(self, client_id: str) -> int:
        if self.settings.demo_mode:
            now = int(datetime.now(UTC).timestamp())
            count, expires_at = self._demo_rate_limits.get(client_id, (0, now + 60))
            if now >= expires_at:
                count, expires_at = 0, now + 60
            count += 1
            self._demo_rate_limits[client_id] = (count, expires_at)
            return count
        result = await self._redis().eval(
            """
            local count = redis.call('INCR', KEYS[1])
            if count == 1 then
                redis.call('EXPIRE', KEYS[1], 60)
            end
            return count
            """,
            1,
            f"rate:{client_id}",
        )
        return int(result)

    async def close(self) -> None:
        if self.settings.demo_mode:
            return
        if self.neo4j is not None:
            await self.neo4j.close()
            self.neo4j = None
        await self.cassandra.close()
        if self.redis is not None:
            await self.redis.aclose()
            self.redis = None
        if self.mongo is not None:
            await self.mongo.close()
            self.mongo = None


async def close_platform(platform: Platform) -> None:
    try:
        await platform.close()
    except Exception:
        logger.exception("Failed to close one or more telemetry data stores")
