# Sleep model budget batching

Sleep batches must fit the same rendered request and output reservation used by
`PrimaryModel`. Observation Events remain immutable; a new run records only a
fitting ledger-order prefix, and its completion cursor advances through that
prefix. The remaining observations are planned in a later cycle.

The Sleep model port provides a network-free request budget preflight. Active
runs keep their recorded seed IDs and fail without cursor advancement when the
current settings no longer fit them.
