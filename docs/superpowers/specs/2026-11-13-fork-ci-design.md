# Fork CI for SmartThingsOSS/redis-kafka-connect

## Intent
Run builds and tests in the SmartThingsOSS fork, and publish the Confluent connector archive
(`redis-kafka-connect:createConfluentArchive`), without ever conflicting with upstream
(redis-field-engineering) when upstream changes are merged into the fork.

## Approaches
1. **Chosen: add fork-only workflow files and leave upstream's alone.** Upstream never touches
   `smartthings-*.yml`, so merges can't conflict. A `github.repository` guard makes them do nothing elsewhere.
2. Edit upstream's `build.yml` and `release.yml`. Rejected: every upstream change to them would conflict.
3. Keep CI on a separate long-lived branch. Rejected: PRs to master wouldn't get CI.

## Design
- `.github/workflows/smartthings-ci.yml`
  - Pull requests: build, unit and integration tests (JDK 27), and the Confluent Platform worker test
    (Java 21). **No publish.**
  - Push to master or a manual run: the same tests, then the Confluent zip is uploaded as a workflow artifact.
- `.github/workflows/smartthings-release.yml`
  - Trigger: push of a tag matching `<major>.<minor>.<patch>-st.<n>`, for example `1.2.0-st.1`.
  - The tag has no `v` prefix, so it never collides with upstream's `v*` tags. Axion only reads `v*` tags,
    and `release.forceVersion` always appends `-SNAPSHOT`. So the job creates a local tag `v<tag>` that it
    never pushes, which makes Axion report exactly `1.2.0-st.1`.
  - Builds and tests, checks that `redis-redis-kafka-connect-<tag>.zip` exists, and runs
    `gh release create` with the zip attached and generated notes.
- Self-contained: the jobs use `actions/*` and `gradle/actions` pinned to release tags, and don't depend on
  `redis/github-workflows@main`.

## One-time settings (not files)
- In the fork's Actions UI, disable upstream's "Build" and "Release" workflows.
- Optionally make "SmartThings CI" checks required on master.

## Keeping upstream PRs clean
- Branches for upstream PRs: `git switch -c issue-NNN upstream/master`, then push to origin and open the PR
  against redis-field-engineering. These branches don't contain the fork CI commit.
- Fork-only work branches from origin/master, and fork CI runs on its PRs.
- Before opening an upstream PR, check `git diff upstream/master...HEAD --stat`.
- Bring upstream into the fork with `git merge upstream/master`. Don't rebase fork master.
