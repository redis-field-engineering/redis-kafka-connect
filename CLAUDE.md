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
./gradlew redis-kafka-connect:allTests

# Create distribution JAR
./gradlew redis-kafka-connect:shadowJar

# Run test coverage
./gradlew redis-kafka-connect:jacocoTestReport

# Check license headers
./gradlew license

# Format license headers
./gradlew licenseFormat

# Create Confluent Hub archive
./gradlew redis-kafka-connect:createConfluentArchive
```

### Testing

The project includes comprehensive test suites:
- Unit tests: `./gradlew test`
- Integration tests with Redis Enterprise and Redis Stack: Use `redis-kafka-connect:allTests`
- Tests use TestContainers for Redis instances

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
4. **Stack Tests**: Test Redis Stack features (JSON, Search, etc.)

### Running Specific Tests

```bash
# Run single test class
./gradlew test --tests "StreamSourceConnectorTest"

# Run integration tests only
./gradlew redis-kafka-connect:allTests -Dtest.include.tags="integration"
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

- **Lettuce**: Redis client library (configured via `lettucemodVersion` in gradle.properties)
- **Kafka Connect API**: For connector framework integration
- **Spring Batch Redis**: For batch processing utilities
- **TestContainers**: For integration testing

### Version Management

Key versions are managed in `gradle.properties`:
- Java compatibility: JDK 17
- Lettuce version: 6.5.2.RELEASE
- TestContainers Redis: 2.2.3

## Release Process

The project uses JReleaser for release management:
- Confluent Hub distribution via `createConfluentArchive` task
- GitHub releases with automated changelog generation
- Version management through Gradle properties