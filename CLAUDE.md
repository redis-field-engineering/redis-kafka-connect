# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is the **Redis Kafka Connect** project - a set of Kafka Connect connectors for Redis that provides both source and sink functionality. The project enables bidirectional data flow between Redis and Apache Kafka.

### Key Components

- **RedisSinkConnector**: Writes data from Kafka topics to Redis (supports regular Redis keys, JSON documents, and streams)
- **RedisStreamSourceConnector**: Reads data from Redis streams and publishes to Kafka topics  
- **RedisKeysSourceConnector**: Monitors Redis key changes via keyspace notifications and publishes to Kafka topics

## Build System

This project uses **Gradle** as the build system with a multi-project structure:

### Core Commands

```bash
# Build the project
./gradlew build

# Run tests
./gradlew test

# Run all tests (including integration tests)
./gradlew integrationTest

# Create distribution JAR
./gradlew redis-kafka-connect:shadowJar

# Create Confluent Hub archive
./gradlew redis-kafka-connect:createConfluentArchive
```

### Testing

The project separates unit and integration test tasks:
- Unit tests: `./gradlew test`
- Integration tests with Redis Enterprise 8.2 and Redis 8.2: Use `integrationTest`
- Tests use Testcontainers for Redis instances
- `./gradlew workerTest` runs the shaded artifact inside Confluent Platform 8.3.2 on Java 21
- Cluster integration tests use ports 17000–17002; the worker broker uses port 19093

## Architecture

### Module Structure

```
core/redis-kafka-connect/
├── src/main/java/com/redis/kafka/connect/
│   ├── RedisKeysSourceConnector.java       # Keys source connector
│   ├── RedisSinkConnector.java            # Sink connector  
│   ├── RedisStreamSourceConnector.java    # Stream source connector
│   ├── common/                            # Shared configuration and utilities
│   │   ├── RedisConfig.java              # Redis connection configuration
│   │   └── RedisConfigDef.java           # Configuration definitions
│   ├── sink/                             # Sink connector implementation
│   │   ├── RedisSinkTask.java           # Main sink task
│   │   └── RedisSinkConfig.java         # Sink configuration
│   └── source/                           # Source connector implementations
│       ├── AbstractRedisSourceConnector.java
│       ├── RedisKeysSourceTask.java     # Keys monitoring task
│       └── RedisStreamSourceTask.java   # Stream reading task
```

### Key Design Patterns

- **AbstractRedisSourceConnector**: Base class for all source connectors, handles common functionality like task configuration and versioning
- **RedisConfig**: Centralized Redis connection configuration supporting standalone, cluster, SSL/TLS, and authentication
- **Task-based architecture**: Each connector delegates actual work to corresponding Task classes
- **Modular configuration**: Each connector has its own ConfigDef for type-safe configuration

## Development Environment

### Docker Development Setup

Use the provided `run.sh` script for a complete development environment:

```bash
./run.sh
```

This script:
- Starts Redis, Kafka, and Kafka Connect via Docker Compose
- Configures sample connectors
- Provides UI access at http://localhost:9021 (Confluent Control Center) or http://localhost:8000

### Manual Docker Setup

```bash
# Start services
docker compose up -d

# Stop services  
docker compose down
```

## Testing Strategy

### Test Categories

1. **Unit Tests**: Test individual components in isolation
2. **Integration Tests**: Test against real Redis instances using TestContainers
3. **Enterprise Tests**: Test Redis Enterprise features
4. **Redis Tests**: Test Redis 8 features, including JSON and TimeSeries

### Running Specific Tests

```bash
# Run single test class
./gradlew test --tests "StreamSourceConnectorTest"

# Run integration tests only
./gradlew integrationTest
```

## Configuration

### Redis Connection

The connectors support multiple Redis deployment types:
- **Standalone**: Single Redis instance
- **Cluster**: Redis Cluster setup
- **SSL/TLS**: Secure connections with certificate validation
- **Authentication**: Username/password and certificate-based auth

### Common Configuration Properties

- `redis.uri`: Redis connection URI (e.g., `redis://localhost:6379`)
- `redis.cluster`: Enable cluster mode
- `redis.tls`: Enable SSL/TLS
- `redis.username`/`redis.password`: Authentication credentials

## Dependencies

### Core Dependencies

- **Lettuce**: Redis client library (versions managed in `gradle/libs.versions.toml`)
- **Kafka Connect API**: For connector framework integration
- **Direct Lettuce commands**: Task-owned batching, stream groups, and keyspace notification readers
- **TestContainers**: For integration testing

### Version Management

Key versions are managed in `gradle/libs.versions.toml`:
- Build and default test runtime: JDK 27
- Connector bytecode and API compatibility: Java 17 (`--release 17`)
- Lettuce: 7.9.0.RELEASE; Netty: 4.2.19.Final
- Native Gradle BOM imports and catalog versions; no Spring Batch, Spring Boot, or LettuceMod
- Testcontainers Redis: 3.0.0; Testcontainers: 2.0.5
- Integration images: `redislabs/redis:8.2.0-125.19` and `redis:8.2.10`
- Override test runtime/images with `-PtestJavaVersion`, `-PredisEnterpriseImage`, and `-PredisImage`

## Release Process

The project uses JReleaser for release management:
- Confluent Hub distribution via `createConfluentArchive` task
- GitHub releases with automated changelog generation
- Version management through Axion Git tags; dependency versions use the Gradle catalog

## Runtime ownership and compatibility

Each task owns its Lettuce client and connections. The sink validates commands before
sending a batch, awaits all pipeline results, and uses a physical primary connection
for each cluster slot's transaction and WAIT. Transactions do not span slots.

Stream checkpoints use stream/group/consumer source partitions with the existing
`offset` field. Legacy empty-partition checkpoints are read for group initialization;
all pending entries replay conservatively, including claimed IDs below a saved checkpoint. Keys subscriptions are established before
SCAN, use a bounded queue, and fail explicitly on overflow, disconnect, or topology
change. Keep Redis callbacks free of blocking I/O. The keys source emits one task only.
