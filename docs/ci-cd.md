# Continuous integration and delivery

The GitHub Actions workflow in `.github/workflows/ci-cd.yml` runs on pull requests
to `main`, pushes to `main`, and manual dispatch. Manual runs validate only.

## Validation

- Install `requirements.txt` and check dependency compatibility on Python 3.10–3.14.
- Compile all application modules and run the unittest suite, including startup
  with temporary configuration and mocked MQTT. No live service credentials are needed.
- After all Python checks pass, build the Python 3.11 Docker image, check its CLI
  entry point, and run the same tests inside that image.

Reproduce the Python checks locally:

```bash
python -m pip install -r requirements.txt
python -m pip check
python -m compileall -q main.py signalk_access.py webapp_wsgi.py tests
python -m unittest discover -s tests -v
```

## Image delivery

Only a successful push build on `main` publishes the tested image to:

```text
ghcr.io/ccmmma/sensor-network-collector:latest
ghcr.io/ccmmma/sensor-network-collector:sha-<full-commit-sha>
```

`latest` tracks successful deliveries; use a commit tag or image digest for a
specific rollout or rollback. Builds use the runner's Linux AMD64 architecture.
Pull requests and manual runs do not publish. New pull request runs cancel older
runs for that pull request; main-branch runs are serialized by workflow concurrency.

The workflow uses the built-in `GITHUB_TOKEN` with `packages: write` in the
container job, following [GitHub's package publishing workflow](https://docs.github.com/en/packages/managing-github-packages-using-github-actions-workflows/publishing-and-installing-a-package-with-github-actions).
No custom registry secret is required. GitHub Actions and package creation must be
allowed by repository/organization policy. If the package already exists, grant
this repository Actions access in its package settings. Set the package visibility
to public if anonymous pulls are desired; otherwise authenticate deployment hosts
with a credential authorized to read the package.

The Docker build context includes only runtime sources, `requirements.txt`, and
Docker build files. Configuration, credentials, databases, CSV data, and local
virtual environments are excluded. Mount runtime configuration and data as shown
in [Docker deployment](docker.md).

## Rollout

Delivery ends at the container registry. The workflow does not connect to a
production host or restart running services.

To use a published image, replace the `build` block in both services of your local
`docker-compose.yml` with the same image reference:

```yaml
image: ghcr.io/ccmmma/sensor-network-collector:sha-<full-commit-sha>
```

Keep the existing commands, configuration mounts, data volumes, and ports. Then:

```bash
docker compose pull collector web
docker compose up -d --no-build collector web
docker compose ps
docker compose logs --tail=100 collector web
```

For rollback, change both image references to a previously validated commit tag
or digest and repeat these commands. Persistent data remains on its mounted volume.
