# CockroachDB Consistency Model (excerpt)

Source: https://www.cockroachlabs.com/blog/consistency-model/

Saved excerpt — the article's formal, history-based definition of linearizability
(Herlihy–Wing style, in terms of invocation/response events). Captured to anchor the
wiki's distributed-consistency pages with the precise definition behind the informal
"real-time recency on a single object" description.

## Linearizability (as defined in the article)

> Here's where we use linearizability: we say that a history H is linearizable if it is
> equivalent to some valid sequential history H', where H' contains the same events,
> possibly reordered under the constraint that, if a response op1 appears before an
> invocation op2 in H, then this order is preserved in H'.
