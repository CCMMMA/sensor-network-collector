# Development and validation

## Local setup

Run commands from the repository root. Python 3.10–3.14 is checked by CI; the
container uses Python 3.11.

```bash
python3 -m venv .venv
source .venv/bin/activate
python -m pip install -r requirements.txt
python -m pip check
```

For live operation, copy `config.json.sample` to `config.json`, configure reachable
services, and replace sample credentials. `config.json` is ignored by Git. Other
configuration filenames are not automatically ignored; keep their credentials out
of commits. See [configuration](configuration.md).

## Automated checks

```bash
python -m compileall -q main.py signalk_access.py tests
python -m unittest discover -s tests -v
git diff --check
```

The suite uses standard-library `unittest` and installed runtime dependencies.
It creates temporary files and mocks MQTT where needed; no running broker, database server, or real credentials are required.

| Test module | Coverage |
| --- | --- |
| `tests/test_storage.py` | Unsafe identifiers, symlink escapes, schema expansion, file permissions, failed replacement cleanup, reserved metadata, and continued sink processing after a CSV rejection |
| `tests/test_startup.py` | Configuration loading and startup with mocked MQTT; legacy web settings (`httpEnabled`, `--http`) are ignored with a warning |
| `tests/test_collector_robustness.py` | MQTT callback resilience, Signal K URL and token persistence fallback |

These checks do not verify live MQTT delivery, InfluxDB writes, or Signal K approval. Use the [operations guide](operations.md)
to validate those integrations in a configured environment.

## Container checks

With Docker running, reproduce the container job locally:

```bash
docker build -t sensor-network-collector:check .
docker run --rm sensor-network-collector:check python main.py --help
docker run --rm --volume "$PWD/tests:/tests:ro" sensor-network-collector:check \
  python -m unittest discover -s /tests -v
```

Tests are mounted into the container; they are not included in the runtime image.
The build context allowlist in `.dockerignore` and explicit `COPY` instructions in
`Dockerfile` exclude configuration and data. If adding a runtime file, update both.
Local builds use the host's architecture; published CI images are Linux AMD64.

## Live startup and dry mode

```bash
python main.py --config config.json --dry
```

Dry mode connects to MQTT and logs would-be sink output, so it needs a reachable
broker and can include payload contents in logs. It suppresses CSV/InfluxDB writes
and Signal K publishing. Stop with Ctrl+C. A dry run with no incoming messages only confirms startup and
broker connectivity, not message processing.

## Change workflow

Keep changes focused, update affected pages and sample configuration in the same
change, and record validation results. Runtime dependencies belong in
`requirements.txt`. Keep runtime logic in `main.py` unless a refactor improves
complexity and testability, as described in `AGENTS.md`.

Pull requests targeting `main` run the [CI/CD workflow](ci-cd.md). Successful pushes
to `main` also publish a container image; manual workflow runs validate only.
