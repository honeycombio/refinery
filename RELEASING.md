# Release Process

Note that it is not necessary to regenerate documentation as part of a standard PR for Refinery. It causes a lot of noise and should only be run immediately before a release. Generated documentation should not be committed in a standard PR, even if a contributor ran `make all` locally to validate their changes.

Licenses are different: a PR that changes dependencies must include the matching `LICENSES` update (CI runs `make verify-licenses`), ideally as its own commit. Dependabot PRs get this automatically.

- Check that licenses are current with `make verify-licenses`
- Regenerate documentation with `make all` from within the `tools/convert` folder.
  - If there have been changes to `rules.md`, you may need to manually modify the `rules_complete.yaml` to reflect the same change.
- If either `refinery_config.md` or `refinery_rules.md` were modified in this release, you must also open a [docs](https://github.com/honeycombio/docs) PR and update these files there under `content/_common/refinery/`.
  - Replace the underscores (`_`) in the filenames with a dash (`-`) or the docs linter will be upset.
  - Address any feedback from the the docs team and apply that feedback back into this repo.
  - For added points, get the docs team to give feedback on docs changes in the original PR that changed them.
- Refresh the PGO profile if the ingest path changed a lot since it was last captured (see [Refreshing the PGO profile](#refreshing-the-pgo-profile)). This is optional for most releases.
- After addressing any docs changes, add release entry to [changelog](./CHANGELOG.md).
  - Use this command to get a list of all commits since last release:

  ```sh
  git log <last-release-tag>..HEAD --pretty='%Creset- %s | [%an](https://github.com/%an)'
  ```

- Copy the output from the command above into the top of [changelog](./CHANGELOG.md)
  - fix each `https://github.com/<author-name>` to point to the correct github username
    (the `git log` command can't do this automatically)
  - organize each commit based on their prefix into below three categories:

    ```md
        ### Features
         - <a-commit-with-feat-prefix>

        ### Fixes
         - <a-commit-with-fix-prefix>

        ### Maintenance
         - <a-commit-with-maintenance-prefix>
    ```

- Add a summary of release changes to [release notes](./RELEASE_NOTES.md).
- Commit changes, push, and open a release preparation pull request for review.
- Once the pull request is merged, fetch the updated `main` branch.
- Apply a tag for the new version on the merged commit (e.g. `git tag -a v1.4.1 -m "v1.4.1"`)
- Push the tag upstream (this will kick off the release pipeline in CI) e.g. `git push origin v1.4.1`
- Ensure that there is a draft GitHub release created as part of CI publish steps.
- Click "generate release notes" in GitHub for full changelog notes and any new contributors
- Publish the GitHub draft release
- Update the `appVersion` and any relevant chart changes in [helm-charts](https://github.com/honeycombio/helm-charts/tree/main/charts/refinery)

## Refreshing the PGO profile

`cmd/refinery/default.pgo` is a CPU profile that `go build` applies automatically ([profile-guided optimization](https://go.dev/doc/pgo)). It changes how the compiler inlines and devirtualizes code, not what Refinery does. The profile is applied at build time only; a running binary cannot load one.

The profile comes from production Refinery. It is dominated by the ingest path: OTLP and msgpack decoding, zstd, and the garbage collector. Rule evaluation and the samplers barely appear in it, so they gain little from it.

The profile ages as the code changes: functions that were renamed or rewritten stop matching and lose the optimization. Nothing breaks. Refresh it when the ingest path has changed substantially, such as after a change to decoding, compression or the router, or when you want to cover new work.

To refresh it:

1. Collect CPU profiles from production Refinery over several days, from a build close to the code you are releasing. A single minute is too noisy; the current profile merges thousands of one-minute profiles.
2. Merge them: `go tool pprof -proto profile1.pb.gz profile2.pb.gz ... > merged.pgo`.
3. Check that the mix still looks right with `go tool pprof -top merged.pgo`. It should not be dominated by one request type. If source files moved a lot since the capture, the line offsets will be off for those functions.
4. Replace `cmd/refinery/default.pgo`, and say in the PR which profiles were merged and for which dates.
5. Check that it is applied: `go build -o /tmp/refinery ./cmd/refinery && go version -m /tmp/refinery | grep pgo` should show `-pgo=.../cmd/refinery/default.pgo`.

To build without it, use `go build -pgo=off`.

The file is binary, so each refresh adds its full size (about 145 KB) to the repository history. Refresh when it is worth it, not on every release.
