# Validation and release workflow

Run `make check` before pushing: Python compilation, shell syntax checks, the
regression suite, and a real CLI smoke test. The smoke test processes the supplied
CSV files into a temporary directory, checks gold keys and references, then reruns
to verify no output contents or modification times changed. It does not change
existing local processing data or access GCP.

GitHub Actions runs on feature/develop/master pushes and pull requests targeting
develop or master. Python 3.10 and 3.12 run tests and smoke checks; a separate job
builds the Docker image and runs the same checks inside it. JUnit reports are
retained for 14 days. Actions are pinned to commit hashes; Dependabot proposes
updates against develop.

After a push to master, and only after both validation jobs pass, the release job
builds and tests the exact image it publishes to
`ghcr.io/<owner>/<repository>:<commit-sha>`. It uses GitHub's job-scoped token with
package write permission. Feature branches, pull requests and manual runs do not
publish images. No GCP credentials are required by these checks.

This is continuous integration and container delivery. Infrastructure deployment
is not configured: choose the runtime, identity and environment before adding a
cloud deployment job that depends on these checks. Existing BigQuery tests mock
cloud commands; a passing CI run does not establish that a live warehouse works.
Dependencies use supported version ranges; the container tag identifies the build,
not a promise that future rebuilds resolve identical dependency versions.

## Branch progression

The initial import is divided into logical features at actual commit times:
source contracts, local cleaning, warehouse models, cloud loading, orchestration,
container tooling, reporting/documentation, and CI. Each feature starts from the
current develop and is merged back with a merge commit. This records reviewable
slices of an existing project, not an invented development timeline.

`master` starts with a small baseline. Review develop before the final release:

```bash
git switch master
git merge --no-ff develop -m "chore(release): promote validated pipeline"
git push origin master
```

That push activates the release job. In GitHub branch settings, configure pull
requests and required status checks (`Python 3.10 checks`, `Python 3.12 checks`,
`Container checks`) for develop/master if your plan supports protection for this
repository. Workflow files alone do not prevent direct pushes or require reviews.
The workflow becomes manually runnable from the Actions tab once it is present
on the repository's default branch.

The publication includes existing assignment evidence as historical artifacts;
those screenshots and cloud counts are not results of the current CI run.
