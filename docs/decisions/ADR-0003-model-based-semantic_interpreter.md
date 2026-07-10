# ADR-0003 — Use a Model-Based Semantic Interpreter

## Status

Accepted

## Decision

Use a cheap, replaceable language model to convert informal text and voice transcripts into a validated TaskDraft.

Do not use keyword or regular-expression classification as the primary natural-language understanding layer.

## Reason

The primary input is informal Russian speech with corrections, omitted context, references such as “do the second option,” and topic-dependent meaning. Keyword routing is too fragile at the most important semantic boundary.

## Consequences

- original input is always retained;
- interpreter output is schema-validated;
- project and reply context are supplied explicitly;
- the interpreter selects capabilities, not credentials;
- Python remains responsible for policy and security;
- an evaluation fixture set is required to compare cheap providers.
