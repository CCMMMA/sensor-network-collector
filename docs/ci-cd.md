# Continuous integration and delivery

The GitHub Actions workflow in `.github/workflows/ci-cd.yml` runs on pull requests
to `main`, pushes to `main`, and manual dispatch. Manual runs validate only.

| Event | Python checks | Docker build and tests | GHCR publication |
| --- | --- | --- | --- |
| Pull request targeting `main` | Yes | After Python checks pass | No |
| Push to `main`, including documentation changes | Yes | After Python checks pass | After container checks pass |
| Manual dispatch | Yes | After Python checks pass | No |

The workflow does not run for tag pushes or ordinary pushes to other branches.
See [development](development.md) for local setup and test coverage.

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
python -m compileall -q main.py signalk_access.py tests
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
Only one pending run is retained per concurrency group, so rapid pushes may
supersede pending runs. Re-running a push workflow can republish its commit and
move `latest`; use a commit tag or digest rather than assuming `latest` identifies
the newest source commit.

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

To use a published image, replace the `build` block of the `collector` service in
your local `docker-compose.yml` with the image reference:

```yaml
image: ghcr.io/ccmmma/sensor-network-collector:sha-<full-commit-sha>
```

Keep the existing command, configuration mount, and data volume. Then:

```bash
docker compose pull collector
docker compose up -d --no-build collector
docker compose ps
docker compose logs --tail=100 collector
```

For rollback, change the image reference to a previously validated commit tag
or digest and repeat these commands. Persistent data remains on its mounted volume.

Images published before the web application moved to `sensor-network-pwa` also
contained Gunicorn and `webapp_wsgi.py`; current images do not. A web service must
not be moved to a current image: see [upgrading](upgrading.md#registry-images).

Image rollback does not undo data or schema changes. Keep a compatible backup as
described in [operations](operations.md#backup-and-restore).

## Inspect and run the workflow

Open the repository's **Actions** tab and select **CI/CD**. Expand a failed job
and step for its logs. Successful publication records image tags in the run summary.
For a manual validation run, select **Run workflow** and choose the branch.

With an authenticated GitHub CLI, run from the repository checkout:

```bash
gh run list --workflow ci-cd.yml --limit 5
gh workflow run ci-cd.yml --ref main
```

Inspect a specific run using `gh run view <run-id>` and failed logs with
`gh run view <run-id> --log-failed`. Manual validation does not publish an image.

The workflow supplies checks but does not configure branch protection or require
reviews. Configure those repository rules separately if needed. The workflow also
does not perform live service integration tests or provide a production health check.

## Troubleshooting

| Failure | Action |
| --- | --- |
| Python dependency install or `pip check` fails | Inspect the failing Python job, reproduce in a clean environment on that version, and update `requirements.txt` if a compatibility constraint is needed. |
| Regression test fails | Run `python -m unittest discover -s tests -v` from the repository root and fix the reported behavior before delivery. |
| Local Docker daemon unavailable | Start Docker before running the container checks; Python checks can run independently. |
| A runtime file is missing from the image | Update both `.dockerignore` and the `Dockerfile` copy list. Tests are mounted separately by CI. |
| GHCR push denied | Check organization package-creation policy and the existing package's Actions access for this repository. |
| Deployment pull denied | Verify package visibility and authenticate the host for private-package reads. |
| No image for a manual or pull request run | Expected: only successful push runs on `main` publish. |
| Image cannot run on an ARM host | Published images target Linux AMD64; use supported emulation or build from source on the target host. |

Dependencies and action version tags are not fully locked, so a later build of the
same source may resolve different upstream versions. Record the delivered digest
when an exact image is required. Publication pushes the commit tag before `latest`;
if the second push fails, inspect both tags before deciding what to deploy.
