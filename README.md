# Mission Telemetry Platform

A runnable capstone implementation for ingesting satellite sensor packets, validating and processing them through Kafka in live mode (or an in-process queue in demo mode), and using a database suited to each access pattern:

- **MongoDB** stores queryable telemetry documents and serves the latest reading for a sensor.
- **Redis** caches current health, stores recent alerts, and enforces API rate limits.
- **Cassandra** stores time-series readings by `(satellite, sensor, UTC day)` for bounded historical queries.
- **Neo4j** stores satellite → module → sensor dependencies for graph traversal.
- **Kafka** buffers live-mode packets, preserves per-satellite ordering, and decouples ingestion from storage.
- **FastAPI** exposes validated, JWT-protected endpoints with mission-control/scientist roles.

This repository provisions a complete local demo with Docker Compose. Its explicit demo mode also runs without Kafka or external databases, using an in-process queue and in-memory projections. Terraform describes the Azure application foundation and Kafka-compatible Event Hubs; the managed databases are external services (MongoDB Atlas, Redis Cloud/Azure Managed Redis, Cassandra-compatible Astra DB, and Neo4j Aura). Their use is optional and costs depend on provider and selected tier. **No Azure resources are created by the local demo.** The [capstone report](docs/CAPSTONE_REPORT.md) contains the architecture rationale, model designs, security, operations, cost worksheet, and demo outline.

## Run locally

Requirements: Docker Desktop with Compose v2 (or a compatible Docker engine), 6 GB+ available memory, and Python 3.12+ if running tests on the host.

1. Copy `.env.example` to `.env`. Change the local mission/scientist passwords and Neo4j password before sharing your machine or exposing ports.
2. Start the services:

   ```powershell
   docker compose up --build
   ```

   First start takes a few minutes, especially while Cassandra initializes. The API waits for each dependency's health check.
3. Open the mission dashboard at <http://localhost:8000>, Swagger at <http://localhost:8000/docs>, and liveness at <http://localhost:8000/health>.
4. To stop services and **keep** local database volumes, press Ctrl+C and run `docker compose down`. To intentionally erase local demo data, use `docker compose down -v`.

The local Compose database images intentionally use development-only settings and are bound to local ports. Do not expose this stack to the public internet.

## First demo

In PowerShell, get a mission-control token and submit one telemetry packet:

```powershell
$token = (Invoke-RestMethod -Method Post http://localhost:8000/v1/auth/token `
  -ContentType 'application/json' `
  -Body '{"username":"mission-operator","password":"change-this-local-development-password"}').access_token

$headers = @{ Authorization = "Bearer $token" }
$packet = @{
  satellite_id = 'SAT-001'
  module_id = 'power'
  sensor_id = 'temp-01'
  metric = 'temperature_c'
  value = 97.2
  unit = 'C'
  observed_at = [DateTime]::UtcNow.ToString('o')
  sequence = 1
} | ConvertTo-Json

Invoke-RestMethod -Method Post http://localhost:8000/v1/telemetry `
  -Headers $headers -ContentType 'application/json' -Body $packet
Start-Sleep -Seconds 2
Invoke-RestMethod http://localhost:8000/v1/alerts -Headers $headers
Invoke-RestMethod http://localhost:8000/v1/satellites/SAT-001/health -Headers $headers
Invoke-RestMethod http://localhost:8000/v1/satellites/SAT-001/dependencies -Headers $headers
```

The default local credentials are for demonstration only. The `scientist` account can query telemetry but cannot submit packets. The API accepts optional caller-generated UUID `event_id` values so retries are idempotent in MongoDB and Cassandra; alert IDs are deterministic for replayed events.

## API

All `/v1` endpoints are rate limited by client IP in Redis. Obtain a bearer token from `POST /v1/auth/token` with a JSON body containing `username` and `password`.

| Method | Path | Role | Purpose |
|---|---|---|---|
| `GET` | `/health` | Public | Liveness check |
| `POST` | `/v1/auth/token` | Public | Issue a short-lived HS256 access token |
| `POST` | `/v1/telemetry` | Mission control | Validate and durably enqueue a packet (202 Accepted) |
| `GET` | `/v1/satellites/{satellite_id}/health` | Either | Read the Redis health snapshot |
| `GET` | `/v1/satellites/{satellite_id}/sensors/{sensor_id}/latest` | Either | Read the latest MongoDB packet |
| `GET` | `/v1/satellites/{satellite_id}/sensors/{sensor_id}/history?start=...&end=...` | Either | Read Cassandra history; requires timezone offsets and is capped at 31 calendar days |
| `GET` | `/v1/satellites/{satellite_id}/dependencies` | Either | Traverse the Neo4j component graph |
| `GET` | `/v1/alerts?limit=50&satellite_id=...` | Either | Read recent Redis alerts |

`/docs` and `/openapi.json` are available for interactive exploration. API roles are intentionally simple demo accounts; production deployments should federate identity through Microsoft Entra ID (or an OIDC provider), use managed identities where supported, and store passwords/keys only in a secret manager.

## Data model and processing

### Packet

See [the JSON Schema](schemas/telemetry-packet.schema.json). Fields include an idempotency UUID, satellite/module/sensor IDs, metric and unit, finite numeric value, timezone-qualified UTC event time, and monotonically increasing source sequence. Unknown fields, invalid names, negative sequences, non-finite values, and timezone-naive timestamps are rejected before enqueueing.

### Pipeline

Live mode: `Satellite → POST /v1/telemetry → FastAPI validation/JWT/RBAC → Kafka → consumer → MongoDB + Cassandra + Neo4j + Redis → query APIs`

Demo mode: `Satellite → POST /v1/telemetry → FastAPI validation/JWT/RBAC → in-process queue → in-memory projections → query APIs`

In live mode, the API responds only after Kafka acknowledges the packet. The consumer persists it to stores and commits its Kafka offset after successful processing. Store writes are idempotent for a stable event ID; a store failure is logged and retried without acknowledging the message. Processing is **at least once**, not a cross-database transaction: a crash can leave temporary partial writes, and consumers/readers should account for eventual convergence. Demo mode is single-instance and ephemeral: queue contents and projections are lost on restart and are not shared between instances.

Kafka partitions by satellite ID to retain per-satellite order. The single local consumer can be scaled into a consumer group, subject to Kafka partition count and database capacity. Poison messages failing schema validation are explicitly logged and discarded; production deployments should route them to a dead-letter topic and alert operators.

### Store-specific access patterns

| Store | Schema/key | Primary read/write |
|---|---|---|
| MongoDB | One telemetry document per event; compound index `(satellite_id, sensor_id, observed_at DESC)` | Latest sensor reading and flexible packet lookup |
| Redis | `health:{satellite}` hash keyed by sensor; bounded `mission:alerts` list; expiring rate-limit counters | Low-latency health, recent alerts, throttling |
| Cassandra | `((satellite_id, sensor_id, day_bucket), observed_at DESC, event_id)`; daily UTC buckets; schema in [CQL](schemas/cassandra.cql) | High-throughput time-series writes and bounded sensor history |
| Neo4j | `Satellite -[:CONTAINS]-> Module -[:HAS_SENSOR]-> Sensor` | Dependency graph traversal and impact analysis |

Cassandra's `SimpleStrategy` replication factor 1 and single-node Compose topology are **local-demo only**. Production Cassandra requires a reviewed replication/network strategy, tested retention and partition-size bounds, backups, TLS, and capacity planning.

Alert rules are deliberately illustrative: `temperature_c >= 80` (warning) / `>= 95` (critical), and `battery_voltage_v <= 20` (warning) / `<= 15` (critical). Move thresholds to reviewed configuration or mission-specific policies before operational use.

## Tests and code quality

```powershell
pip install -e ".[dev]"
pytest
ruff check .
```

The automated suite checks packet validation, UTC/timezone handling, deterministic event alerting, unknown metrics, and threshold boundary values. Run service integration tests against the Docker Compose stack after adding additional integration coverage.

## Azure deployment with Terraform

The `terraform/` directory describes an Azure resource group, Linux App Service for Containers, a project-owned Basic ACR, Azure Event Hubs, Key Vault, a user-assigned identity, Log Analytics, and a monthly resource-group budget alert. App Service deployment requires available App Service plan quota. When external database endpoints are not available, **demo mode** uses an in-process queue and in-memory projections for MongoDB/Redis/Cassandra/Neo4j access patterns; it does not require Event Hubs. Demo telemetry, health, alerts, and graph data are lost when the app restarts and are not shared across instances. This is for demonstration only and is not the durable multi-model capstone deployment.

1. Create an isolated Terraform state backend with encryption, access control, and state locking. Terraform state includes generated credentials and Event Hubs secrets; never commit state.
2. From `terraform/`, initialize and validate with `terraform init`, `terraform fmt -check`, `terraform validate`, and `terraform plan`. Review the complete plan and estimated spend before applying.
3. Build the image remotely with `az acr build --registry <registry-name> --image mission-telemetry:mission-v1 ..`.
4. Apply the reviewed Terraform plan. Terraform creates the resources and stores runtime credentials in Key Vault.
5. The App Service uses a user-assigned identity and Key Vault references. Create the Cassandra keyspace/table and Neo4j constraints using the [schema files](schemas/) before production; grant runtime accounts only the required read/write permissions. Review ingress, retention, backups, database firewall rules, and service-specific scaling before production.
6. Check App Service logs and diagnostics in the linked Log Analytics workspace. The budget alert is a notification, **not** a spend cap or automatic shutdown.

Azure deployments require an active subscription and can incur charges. The App Service plan defaults to one B1 instance in West US 2 because West US has no available B1 quota. The Terraform module sets a budget notification, not a spending cap. Review each provider's current pricing and quota before applying. Demo login passwords are generated automatically and stored in Key Vault; they can be retrieved from the sensitive Terraform outputs after deployment.

## CI

GitHub Actions in `.github/workflows/ci.yml` runs tests/lint, validates Terraform, and builds the API image. On pushes to `main`, it publishes the image to GitHub Container Registry. Azure deployment remains a reviewed Terraform apply rather than silently provisioning cloud resources from CI.

## Demo video outline (10–15 minutes)

1. State the mission-control problem and throughput/latency requirements.
2. Explain why packet documents, ephemeral health, time-series history, and fault dependencies use four different models.
3. Walk through `schemas/telemetry-packet.schema.json`, the Cassandra daily partition, Redis health keys, and graph relationships.
4. Submit normal and threshold-breaching telemetry packets; show API 202, alert creation, health, history, and dependency endpoints.
5. Trace Kafka buffering, consumer retries/offsets, and the at-least-once consistency trade-off.
6. Show JWT roles, invalid-packet rejection, Redis rate limits, TLS/secret handling, and Azure/Log Analytics Terraform.
7. Close with limitations: demo replication, externally provisioned data services, budgets versus hard limits, dead-lettering, retention, load testing, and production identity.

# Almabetter_Frontend_Project_3
