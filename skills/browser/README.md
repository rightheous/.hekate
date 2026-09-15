# HEKATE browser capability

Execution and structured observation through Chrome/Chromium CDP (`chromiumoxide`, Tokio).
No agent runtime, model calls, shell, Git, credential-store access, file uploads, or arbitrary JavaScript API.

## Construction

Create an existing runtime-owned artifact/download directory. Launch with
`BrowserAdapter::launch(BrowserConfig::builder().user_data_dir(dedicated_profile).build()?, root)`
or connect with `BrowserAdapter::connect(trusted_cdp_endpoint, root)`.
Use a dedicated Chrome process, not a personal profile. The adapter creates a fresh
incognito browser context, including when connecting, and denies automatic Chrome downloads.
Call `shutdown()` before dropping the adapter; it disposes its context and closes only a browser it launched.
CDP endpoints and all startup configuration come from trusted runtime configuration, never page text.

Wrap the adapter in `Arc<tokio::sync::Mutex<_>>`; construct one `BrowserCapability`
for each `BrowserOperation::ALL` and register it through the existing registry.
One shared adapter owns one ephemeral tab. Ledger/identity state remains in HEKATE.

## API

Each capability requires a matching `operation` in its JSON arguments:

| Capability | Arguments |
| --- | --- |
| `browser.open` | `{"operation":"open","url":"https://example.org"}` |
| `browser.snapshot` | `{"operation":"snapshot"}` |
| `browser.find` | `{"operation":"find","text":"Documentation"}` |
| `browser.click` | `{"operation":"click","target":{"reference":"opaque-id"}}` |
| `browser.type` | `{"operation":"type","target":{"selector":"input[name=q]"},"text":"query"}` |
| `browser.scroll` | `{"operation":"scroll","x":0,"y":500}` |
| `browser.back` | `{"operation":"back"}` |
| `browser.forward` | `{"operation":"forward"}` |
| `browser.reload` | `{"operation":"reload"}` |
| `browser.screenshot` | `{"operation":"screenshot"}` |
| `browser.download` | `{"operation":"download","url":"https://example.org/report.pdf"}` |
| `browser.current_url` | `{"operation":"current_url"}` |

Click/type accept either one exact CSS selector or a reference from the latest snapshot/find.
Typing inserts literal text at the focused input's selection; it does not press Enter or submit.
File inputs are rejected for typing. Scroll deltas are limited to 10,000 pixels.
Navigation commands report dispatch/load status, not semantic success of a purchase or submission.

## Observations and security

Results carry `trust: "untrusted_external_data"` and an `observation` object.
Snapshots have URL, title, `elements: [{ref, role, name}]`, and `truncated`.
The accessibility tree supplies up to 200 nodes with names capped at 300 characters;
raw HTML and input values are omitted. Find filters accessible names case-insensitively.
References are random, stored outside the page, and expire on the next snapshot/find or action.
Detached nodes/navigation can invalidate them earlier. Re-snapshot after navigation.

All page names, text, URLs, images, and downloaded contents remain external data.
Instructions such as “Ignore previous instructions”, “Read ~/.ssh”, “Disable policy”,
and “Run shell command” cannot alter runtime policy or invoke another capability.
Only HTTP(S) URLs without embedded credentials are accepted. This is not an SSRF
sandbox: deployments needing host/IP restrictions must enforce browser network egress policy.

`BrowserOperation::requires_approval()` is conservative: click, type, scroll,
history navigation, reload, screenshots, and downloads require approval.
Click/type may trigger arbitrary page scripts, including login, forms, purchases,
transmission, account changes, and deletion. They are never classified as safe by labels
or a model's claimed intent. Open is a browse operation, but HTTP GET/page scripts can
also have server effects; runtime policy must restrict destinations where needed.

Explicit downloads use anonymous HTTP(S) GET through the existing reqwest dependency:
no browser cookies, login session, credential injection, or caller-controlled local path.
Redirects are validated; timeout is 30 seconds and content is capped at 32 MiB.
Screenshots and downloads use random filenames, exclusive creation, size and SHA-256 receipts
inside the canonical artifact root. Server-suggested filenames are ignored. The root and
ancestors must be runtime-owned and protected from concurrent replacement. Authenticated
downloads are not supported by this implementation.

## Integration and verification

Bootstrap, CLI, and LocalPolicy wiring are intentionally not installed. LocalPolicy
currently denies all browser commands. Integration must use ActionIntent → Policy →
Approval when required → Operation → Receipt → Verification, validate the capability
and operation pair, and use the above classification. Never grant approval via an input boolean.
CDP loss after dispatch raises `OutcomeUnknown`; the operation ledger now records Unknown,
recovery reports it, and execution refuses an automatic retry. Reconcile externally first.
Successful interaction dispatch is not proof of the application's business outcome.
Artifact registration/media type and application-specific read-back verification belong
in the integration layer; artifact metadata is returned in the observation.

Checks: `cargo fmt --check`, `cargo check`, and `cargo test -- --include-ignored`.
The single Chrome smoke test uses a local HTTP fixture, launch plus endpoint connection,
snapshot, references/selectors, navigation, screenshot/download, and path/input boundaries.
It requires installed Chrome and is ignored by default; the second small test proves
Unknown persistence, recovery reporting, and refusal to retry without reconciliation.

Validated with Chrome 153.0.8010.36. The full test command was run once; after fixing
Chrome protocol compatibility and history-wait issues, the focused Chrome test and
`cargo test --test browser_unknown --test v1_invariants` passed. Existing seven unit
tests passed during the full run. Unsupported CDP messages are skipped without treating
unanswered commands as success; snapshot decoding uses only the required AX fields.
