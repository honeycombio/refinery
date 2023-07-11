# Release Process

1. Add release entry to [changelog](./CHANGELOG.md)
2. Add a summary of release changes to [release notes](./RELEASE_NOTES.md)
3. Open a PR with the above, and merge that into main
4. Create new tag on merged commit with the new version (e.g. `v1.4.1`)
5. Push the tag upstream (this will kick off the release pipeline in CI)
6. Copy change log entry for newest version into draft GitHub release created as part of CI publish steps
7. Update the `appVersion` and any relevant chart changes in [helm-charts](https://github.com/honeycombio/helm-charts/tree/main/charts/refinery)
8. If either `refinery_config.md` or `refinery_rules.md` were modified in this release, you must also copy these files to docs and do a docs PR.
