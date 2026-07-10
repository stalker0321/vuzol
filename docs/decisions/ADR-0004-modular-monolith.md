# ADR-0004 — Start as a Modular Monolith

## Status

Accepted

## Decision

Deploy the MVP as one modular Python application, worker processes, and PostgreSQL.

Logical modules remain isolated, but they are not separate network services by default.

## Reason

The current system serves one user on a 2 vCPU, 4 GB VPS. Microservices would add memory, deployment, networking, and debugging overhead without useful scale.

## Consequences

- internal module boundaries and interfaces are important;
- independent worker processes are allowed;
- future remote execution uses an execution-backend interface;
- a logical architecture diagram must not be interpreted as one container per box.
