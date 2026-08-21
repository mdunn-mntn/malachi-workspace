# Engine run log

One line per pipeline run: `<date> | stage=<HARVEST|...> | candidates=<n> | adopted=<n> | rolled_back=<n> | cost_usd=<n> | note`.
The engine harvests THIS file too — it observes itself with the same instruments it points at everything else.

2026-08-21 | stage=SCAFFOLD | candidates=0 | adopted=0 | rolled_back=0 | cost_usd=0 | engine v0 dirs + FLOORS + config created (keyless)
2026-08-21 | stage=HARVEST | candidates=15 | adopted=0 | rolled_back=0 | cost_usd=0 | v0 harvest+miner+seed+entropy built; counts verified vs manual grep
