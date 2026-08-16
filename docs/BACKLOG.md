# Engineering backlog

This file records agreed follow-up work that is important but intentionally
deferred. Completed work belongs in `CHANGELOG.md`.

## Next after Telegram dogfood testing

### Replace fixed diff-size failure with chunked independent review

**Priority:** first engineering task after the current end-to-end Telegram
dogfood pass is complete.

The current independent reviewer accepts a complete diff up to 120,000
characters and fails closed above that boundary. The bound protects provider
context and spend, but a large valid work item must not require human recovery
solely because its diff crossed an arbitrary character count.

Implement a bounded review pipeline that:

1. runs trusted mechanical gates over the complete retained result;
2. separates generated files, lockfiles, and other review noise without hiding
   their presence or bypassing mechanical checks;
3. partitions the remaining diff on file or coherent logical boundaries;
4. independently reviews every partition;
5. aggregates all findings into one result-level verdict, retaining file and
   line provenance;
6. limits automatic review batches or repair cycles rather than treating diff
   size itself as a task failure;
7. asks the worker to split an exceptionally large change before involving the
   user, and escalates only after the bounded automatic recovery is exhausted.

Acceptance requires tests proving that a diff above the former 60,000-character
boundary completes review automatically, findings from separate chunks are not
lost, a blocking finding blocks the aggregate verdict, generated/noisy files do
not consume the whole model context, and the batch cap terminates deterministically.

