# Independent review input admission

Both review entrypoints (supervised CLI executor and API worker) pass the existing
`limits.provider_call_input_tokens` setting to `IndependentModelReviewer`.
Every partition and the cross-partition assessment use the smaller of that
configured admission cap and an explicitly configured profile `context_limit`.
An absent profile context limit does not imply an undocumented 32k model cap.
Direct callers retain the conservative 32k default for compatibility.

Previously both request constructors silently clamped to 32k even when the
system allowed 100k per call. A validated repair could grow the retained diff
enough to block the next review before any provider call, although the complete
request fitted the actual configured policy. This was an application wiring
error, not a provider authentication or availability failure.

The fix does not change any configured limit, model, account or routing policy.
Frozen discussion/specification chunks and retained diff chunks remain complete
and hash-verified. Requests exceeding either real cap still fail closed before
reservation/inference; no truncation, budget reset or synthetic verdict is used.
CLI ceilings remain admission estimates, not enforcement of a native agent's
entire subsequent tool loop. Normal budget accounting and task caps still apply.

Regression tests cover explicit/absent/smaller profile caps, lower configured
caps, full contexts beyond 32k, multi-partition and cross-partition requests,
both entrypoint compositions and rejection before reservation/inference.
