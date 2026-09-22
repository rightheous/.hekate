# State machines

Runs move `pending -> running -> suspended/completed/failed/needs_attention`; a suspended or attention run may resume. Attempts are `started -> succeeded/failed/unknown`.

Operations move `planned -> authorized -> started -> succeeded -> verified`, with failure or unknown branches. An unknown started operation is surfaced during recovery and is never silently retried. Approval moves `pending -> approved/denied/expired`.

Completion claims move `needs_validation -> verified/rejected`; terminal dispositions cannot be reopened. A Task completion gate requires every required criterion's latest claim to be verified with valid evidence and blocks pending approvals or unknown operations.

Memory moves `candidate -> promoted/rejected/superseded/expired`; a promoted candidate creates an active memory whose lifecycle is `active -> superseded/expired`.
