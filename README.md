#  Relict Core

**An asynchronous Python framework for simulating proactive AI entities with configurable personalities and motivations.**

`relict-core` is a micro-framework for building the engine behind stateful, agentic AI characters. It provides the architectural foundation to create personalities that don't just *react* to users, but *live* alongside them, driven by their own internal goals and a unique "rhythm of life".

## Manifesto — The Philosophy

Traditional chatbots are reactive puppets. `relict-core` is designed to build proactive **agents**. An entity built with Relict has its own will, driven by three core principles:

1.  **Agency over Reactivity:** The entity is proactive. It can initiate conversations and act on its own schedule, creating the illusion of a separate, thinking consciousness.
2.  **Motivation over Service:** The entity's behavior is dictated by a configurable set of rules and a core goal (e.g., a `personality.json`). Its internal state (e.g., "relationship score") is a metric of its own evaluation, not user satisfaction.
3.  **Engine vs. Configuration:** The Core is a faceless engine. The "soul" of the character—its personality, rules, and goals—is injected entirely through configuration, allowing developers to create vastly different entities without altering the core logic.

## Key Features

-   ** Proactive Agency:** Powered by a `PulsePlanner`, entities have their own "heartbeat," allowing them to act independently on a life-like, non-deterministic schedule.
-   ** Configurable Motivation System:** The core of each personality is defined in a configuration file, dictating how its internal state changes based on user interactions. This dynamically alters the entity's behavior, tone, and decisions.
-   ** Long-Term Memory:** Entities persist key facts and interactions in a PostgreSQL database, allowing them to reference past events across days or weeks.
-   ** Resilient SAGA Architecture:** Inspired by microservices, the engine is composed of independent, stateless workers communicating via Redis Streams. This ensures high availability and fault tolerance.
-   ** Pluggable Layers:** Built with dependency injection, allowing developers to easily swap out LLM providers or databases.

## Architectural Principles

-   **Separation of Concerns:** Clear boundaries between `Workers` (business logic), `Drivers` (external services), and `Databases`.
-   **Event-Driven Communication:** All components are decoupled and communicate asynchronously via Redis Streams (Choreography-based SAGA).
-   **Fail Fast & Resilience:** Each worker runs as an independent process. A critical error will crash a single worker, which can be automatically restarted, while the rest of the system remains operational.
-   **Dependency Injection:** Dependencies are explicitly injected, making components highly testable and configurable.

## Technology Stack

-   **`Python 3.12+`** 
-   **`Pydantic V2`** 
-   **`PostgreSQL`** 
-   **`Redis`** 
-   **`APScheduler`** 
-   **`Docker & Docker Compose`** 
