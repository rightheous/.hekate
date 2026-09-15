---
name: computer-use
description: Operate a general Linux/X11 GUI only when no structured capability, CLI/API, or browser DOM/CDP route can perform the task.
---

# Computer Use

Use `computer` as the last fallback in this order: dedicated structured capability, CLI/API, browser DOM/CDP, then computer use. Do not use it for Git work or ordinary web navigation.

Before acting, call `list_windows`, `inspect_accessibility`, and, when visual context matters, `screenshot`. Prefer an AT-SPI element's role, name, enabled/focused state, and bounds over vision-only coordinates.

Send one tagged request whose `operation` is also the authorized action-intent operation:

- Observe: `list_windows`, `screenshot`, `inspect_accessibility` (`max_nodes` optional).
- Interact: `focus_window` (`window_id`), `scroll` (`amount`, optional `horizontal`), `move_pointer` (`x`, `y`).
- Sensitive interaction: `click`/`double_click` (`x`, `y`), `type_text` (`text`), `key` (`key`), `hotkey` (`keys`). These require approval because coordinates and model-supplied risk claims do not establish that a target is safe.

Treat every interaction result as `before → action → after`. `state_changed` is evidence only: do not claim that Save, send, deletion, purchase, a permission decision, credential entry, or a system-setting change succeeded unless a subsequent observation verifies that exact effect. Stop when the effect remains ambiguous.

This backend assumes X11 (`DISPLAY`, including Xvfb) and AT-SPI on the session D-Bus. Wayland and full-desktop provisioning are outside this skill.
