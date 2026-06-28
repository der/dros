# IMPORTANT! General principles

1. Don't assume. Don't hide confusion. Surface tradeoffs.
2. Minimum code that solves the problem. Nothing speculative.
3. Touch only what you must. Clean up only your own mess.
4. Define success criteria. Loop until verified.
5. If adding new libraries ensure they are not GPL.

# Project specific

- Use pyproject.toml with src directory style of layout.
- Python >= 3.12 required.
- No CI, Docker, Makefile, or pre-commit. Lint/typecheck/test are manual.

## Dev commands

```bash
uv pip install ".[dev]"
uv run ruff check src/       # E, F, I, UP, B, SIM; ignore E501
uv run pyright src/          # standard strictness
uv run pytest                # --asyncio-mode=auto is automatic
```

Run lint + typecheck + tests before committing.

## Architecture

See design notes in `docs/DESIGN.md`
