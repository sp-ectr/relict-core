# Relict Core

Async Python framework for building stateful LLM agents with configurable behavior and personality.

`relict-core` is a micro-framework for running stateful LLM agents. It provides a runtime for long-running entities that can process messages and execute scheduled actions using an internal cycle.

## Architecture

Relict is based on a simple event-driven model where behavior is defined by configuration and internal state.

### 1. Agency over reactivity
The system runs on an internal cycle (pulse) and can execute actions without external input.

### 2. Behavior via configuration
Agent behavior is defined by a configuration file or schema (`PersonalityManifest`).
Internal state (e.g. relationship score) influences decisions.

### 3. Engine/config separation
The core runtime is generic. All behavior is defined externally via configuration.

## Key Features

- **Scheduled execution (Pulse cycle):** agents can act on their own schedule
- **Config-driven behavior:** personality and rules defined via configuration
- **Long-term memory:** state stored in PostgreSQL
- **Event-driven workers:** Redis Streams-based processing model
- **Pluggable integrations:** dependency injection for LLMs and storage

## Architecture

- Workers: stateless event processors
- Redis Streams: message transport
- PostgreSQL: persistence
- Drivers: external integrations

## Principles

- Separation of concerns between workers, drivers, and storage
- Async event-driven communication via Redis Streams
- Fault isolation per worker process
- Explicit dependency injection

## Tech Stack

- Python 3.12+
- Pydantic v2
- PostgreSQL
- Redis
- APScheduler
- Docker / Docker Compose