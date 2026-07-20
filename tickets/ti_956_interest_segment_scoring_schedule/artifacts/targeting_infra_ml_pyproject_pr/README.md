# targeting-infra-ml pyproject — PR #57 (slimmed to a pointer)

The staged package files that used to live here (`pyproject.toml`, `utils/__init__.py`,
`utils/segment_quality_utils/__init__.py`) were a local mirror of what went into a single PR against
`SteelHouse/targeting-infra-ml`. The code copy is no longer the source of truth, so it was removed on
2026-07-20 (audit cleanup) to avoid a stale duplicate of repo code that drifts from upstream.

**Source of truth:** `github.com/SteelHouse/targeting-infra-ml/pull/57` — declares the package + build
backend and adds `utils` / `utils/segment_quality_utils` as packages so setuptools finds them. See this
ticket's `summary.md` for the surrounding context.
