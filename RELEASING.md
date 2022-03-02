# Release Process

1. Add release entry to [changelog](./CHANGELOG.md)
2. Open a PR with the above, and merge that into main
3. Create new tag on merged commit with the new version (e.g. `v1.4.1`)
4. Push the tag upstream (this will kick off the release pipeline in CI)
5. Copy change log entry for newest version into draft GitHub release created as part of CI publish steps
6. Update the `appVersion` and any relevant chart changes in [helm-charts](https://github.com/honeycombio/helm-charts/tree/main/charts/refinery)
