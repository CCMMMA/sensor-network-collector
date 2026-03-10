# AGENTS.md

## Project Overview
- Repository: `sensor-network-collector`
- Purpose: MQTT collector for the `meteo@uniparthenope` sensor network.
- Entry point: `main.py`
- Language: Python 3

## Setup
1. Create a virtual environment.
2. Install dependencies:
   - `pip install -r requirements.txt`

## Run
- Start the collector with:
  - `python main.py --config config.json`

## Development Guidelines
- Keep changes focused and minimal.
- Prefer small, reviewable commits.
- Do not commit secrets, credentials, or environment-specific values.
- Update `README.md` when behavior or configuration changes.
- Keep `docs/*.md` aligned with current behavior for Web GUI, Signal K, storage, and deployment flows.

## Validation
- For each change, at minimum:
  - Ensure `python main.py --config config.json` starts without syntax/runtime import errors.
  - Refresh Markdown docs when user-visible behavior, URLs, units, or configuration semantics change.
  - Verify dependency updates are reflected in `requirements.txt`.

## File/Scope Conventions
- Put project-wide runtime logic in `main.py` unless a refactor is explicitly requested.
- Add new modules only when they reduce complexity and improve testability.
