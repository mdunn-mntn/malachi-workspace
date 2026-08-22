---
name: reference_anthropic_api_key_keychain
description: "ANTHROPIC_API_KEY lives in the macOS login Keychain, not plaintext in ~/.zshrc — zshrc resolves it via `security find-generic-password`; the no-local-key rule is Pi/prod only and the Mac key is legitimate (airflow_debugger/synth.py, AUDI-1191)"
metadata:
  node_type: memory
  type: reference
doc_type: memory
keywords: [ANTHROPIC_API_KEY, macos keychain, security find-generic-password, add-generic-password, anthropic_api_key service name, zshrc secret, airflow_debugger synth.py, AUDI-1191, empty api key, no key on the pi, llm orchestration credential]
domain: [workflow, infra]
lifecycle: active
last_verified: 2026-08-19
---

**`ANTHROPIC_API_KEY` is no longer a plaintext line in `~/.zshrc` (moved 2026-08-19).** It lives in the
macOS **login Keychain** under account `$USER`, service **`anthropic_api_key`**, and `~/.zshrc` resolves
it at shell init:

```bash
export ANTHROPIC_API_KEY=$(security find-generic-password -a "$USER" -s anthropic_api_key -w 2>/dev/null)
```

Read it, or replace it after a rotation:
```bash
security find-generic-password -a "$USER" -s anthropic_api_key -w
security add-generic-password -U -a "$USER" -s anthropic_api_key -w '<new key>'
```

**If the var comes back empty**, the Keychain item is missing or locked, not the zshrc line. The `2>/dev/null`
swallows the `security` error, so an unset var is the only symptom. Verified 2026-08-19 that a fresh login
shell resolves the full 108-char key.

**The Mac key is legitimate. The rule is Pi/prod only.** `.claude/CLAUDE.md` says "no `ANTHROPIC_API_KEY`
on the Pi, ever", and I misread that this session as "no key anywhere" and told the user to delete it. The
real consumer is **`airflow_debugger/synth.py`** (AUDI-1191), which calls the Anthropic Messages API for the
one bounded synthesis call; its design deliberately puts the credential on the Mac and keeps the in-worker
prod path key-free. See [[project_airflow_debugger]], [[reference_pi5_server]].

Same pattern is the right home for any other secret still sitting plaintext in the shell profile.

## A second Keychain secret: the Astro deployment token (2026-08-21)

Same pattern, same reasoning. `astro_deployment_token` in the login Keychain, resolved in
`~/.zshrc` alongside the Anthropic key:

```bash
export AIRFLOW_BEARER=$(security find-generic-password -a "$USER" -s astro_deployment_token -w 2>/dev/null)
export AIRFLOW_TI_API_URL="https://cmd6bd10c0gl901rfuokgryiq.iq.astronomer.run/dokgryiq/api/v2"
export AIRFLOW_API_BASE="$AIRFLOW_TI_API_URL"
```

`DEPLOYMENT_ADMIN` on `airflow-ti`, expires 2027-08-21. **This is the non-human path**:
`airflow_api.resolve_bearer()` prefers `$AIRFLOW_BEARER` over the personal `astro login` context
and never auto-renews an explicitly supplied token, so it does not silently fall back to a person.

**The base URL is NOT what `astro deployment inspect` prints plus `/api/v2` in the obvious way** —
it is `https://<deployment-id>.iq.astronomer.run/<suffix>/api/v2`, where the suffix is the tail of
the id. Take it from `inspect`, do not construct it.

**Never paste a token into a chat, a ticket, or a commit.** This one was pasted, and the honest
status is that it should be rotated (`astro deployment token rotate --deployment-id <id>`); it was
stored on an explicit instruction to keep it. Rotation is a one-liner and the Keychain entry is
the only thing to update.
