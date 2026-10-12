import asyncio
import logging
import ssl
from time import perf_counter

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from aiokafka.structs import ConsumerRecord, OffsetAndMetadata, TopicPartition
from pydantic import ValidationError

from mission_telemetry.alerts import evaluate_alert
from mission_telemetry.models import TelemetryPacket
from mission_telemetry.settings import Settings
from mission_telemetry.stores import Platform

logger = logging.getLogger(__name__)


class TelemetryPipeline:
    def __init__(self, settings: Settings, platform: Platform) -> None:
        self._settings = settings
        self._platform = platform
        self._producer: AIOKafkaProducer | None = None
        self._consumer: AIOKafkaConsumer | None = None
        self._consumer_task: asyncio.Task[None] | None = None
        self._demo_queue: asyncio.Queue[TelemetryPacket] | None = None

    async def start(self) -> None:
        if self._settings.demo_mode:
            self._demo_queue = asyncio.Queue()
            self._consumer_task = asyncio.create_task(
                self._consume_demo(), name="demo-telemetry-consumer"
            )
            return

        bootstrap_servers = self._settings.kafka_bootstrap_servers
        security: dict[str, object] = {}
        if self._settings.kafka_sasl_username and self._settings.kafka_sasl_password:
            security = {
                "security_protocol": "SASL_SSL",
                "sasl_mechanism": "PLAIN",
                "sasl_plain_username": self._settings.kafka_sasl_username,
                "sasl_plain_password": self._settings.kafka_sasl_password.get_secret_value(),
                "ssl_context": ssl.create_default_context(),
            }
        self._producer = AIOKafkaProducer(
            bootstrap_servers=bootstrap_servers,
            acks="all",
            enable_idempotence=True,
            **security,
        )
        await self._producer.start()
        self._consumer = AIOKafkaConsumer(
            self._settings.kafka_topic,
            bootstrap_servers=bootstrap_servers,
            group_id="mission-telemetry-processors-v1",
            enable_auto_commit=False,
            auto_offset_reset="earliest",
            **security,
        )
        try:
            await self._consumer.start()
        except Exception:
            await self._producer.stop()
            self._producer = None
            raise
        self._consumer_task = asyncio.create_task(self._consume(), name="telemetry-consumer")

    async def publish(self, packet: TelemetryPacket) -> None:
        if self._settings.demo_mode:
            if self._demo_queue is None:
                raise RuntimeError("Telemetry pipeline is not running")
            await self._demo_queue.put(packet)
            return
        if self._producer is None:
            raise RuntimeError("Telemetry pipeline is not running")
        await self._producer.send_and_wait(
            self._settings.kafka_topic,
            packet.model_dump_json().encode(),
            key=packet.satellite_id.encode(),
        )

    async def _consume(self) -> None:
        if self._consumer is None:
            raise RuntimeError("Telemetry consumer is not running")
        async for message in self._consumer:
            while True:
                try:
                    try:
                        packet = TelemetryPacket.model_validate_json(message.value)
                    except ValidationError:
                        logger.exception(
                            "Discarding invalid Kafka telemetry message at offset %s",
                            message.offset,
                        )
                        await self._commit_message(message)
                        break
                    await self._project(packet)
                    await self._commit_message(message)
                    break
                except asyncio.CancelledError:
                    raise
                except Exception:
                    logger.exception(
                        "Telemetry persistence failed at Kafka offset %s; retrying",
                        message.offset,
                    )
                    await asyncio.sleep(2)

    async def _consume_demo(self) -> None:
        if self._demo_queue is None:
            raise RuntimeError("Demo telemetry queue is not running")
        while True:
            packet = await self._demo_queue.get()
            try:
                while True:
                    try:
                        await self._project(packet)
                        break
                    except asyncio.CancelledError:
                        raise
                    except Exception:
                        logger.exception(
                            "Demo telemetry persistence failed for event_id=%s; retrying",
                            packet.event_id,
                        )
                        await asyncio.sleep(2)
            finally:
                self._demo_queue.task_done()

    async def _project(self, packet: TelemetryPacket) -> None:
        started = perf_counter()
        await self._platform.persist(packet)
        alert = evaluate_alert(packet)
        if alert is not None:
            await self._platform.record_alert(alert)
            logger.warning(
                "Telemetry alert: satellite=%s sensor=%s metric=%s severity=%s",
                alert.satellite_id,
                alert.sensor_id,
                alert.metric,
                alert.severity,
            )
        processing_ms = round((perf_counter() - started) * 1000, 2)
        if processing_ms >= 1000:
            logger.warning(
                "Slow telemetry projection: satellite=%s event_id=%s duration_ms=%.2f",
                packet.satellite_id,
                packet.event_id,
                processing_ms,
            )

    async def _commit_message(self, message: ConsumerRecord) -> None:
        if self._consumer is None:
            raise RuntimeError("Telemetry consumer is not running")
        await self._consumer.commit(
            {
                TopicPartition(message.topic, message.partition): OffsetAndMetadata(
                    message.offset + 1, ""
                )
            }
        )

    async def close(self) -> None:
        if self._consumer_task is not None:
            self._consumer_task.cancel()
            try:
                await self._consumer_task
            except asyncio.CancelledError:
                pass
            self._consumer_task = None
        if self._consumer is not None:
            await self._consumer.stop()
            self._consumer = None
        if self._producer is not None:
            await self._producer.stop()
            self._producer = None
