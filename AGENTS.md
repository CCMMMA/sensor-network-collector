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
- Automatically update every affected document when code changes modify behavior, configuration, routes, APIs, data flow, UI/UX, deployment, storage, or integration semantics.
- Always review whether `README.md`, `docs/*.md`, sample configuration files, and other user-facing documentation are affected by the code change, and update them in the same work item.
- Do not leave documentation updates as an optional follow-up when the code modification changes anything user-visible or operator-visible.
- Update `README.md` when behavior or configuration changes.
- Keep `docs/*.md` aligned with current behavior for Web GUI, Signal K, storage, and deployment flows.

## Validation
- For each change, at minimum:
  - Ensure `python main.py --config config.json` starts without syntax/runtime import errors.
  - Refresh all affected Markdown and sample-configuration documents when user-visible behavior, URLs, units, configuration semantics, workflows, or operational guidance change.
  - Verify dependency updates are reflected in `requirements.txt`.

## File/Scope Conventions
- Put project-wide runtime logic in `main.py` unless a refactor is explicitly requested.
- Add new modules only when they reduce complexity and improve testability.
