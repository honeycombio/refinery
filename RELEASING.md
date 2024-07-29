# Release Process

1. Check that licenses are current with `make verify-licenses`
2. Regenerate documentation with `make all` from within the `tools/convert` folder. If there have
been changes to `rules.md`, you may need to manually modify the `rules_complete.yaml` to reflect the same change.
3. If either `refinery_config.md` or `refinery_rules.md` were modified in this release, you must also open a [docs](https://github.com/honeycombio/docs) PR and update these files there under `layouts/shortcodes/subpages/refinery/` .
   Replace the underscores (`_`) in the filenames with a dash (`-`) or the docs linter will be upset.
   Address any feedback from the the docs team and apply that feedback back into this repo.
4. After addressing any docs change, add release entry to [changelog](./CHANGELOG.md)
    - Use below command to get a list of all commits since last release
    ```
        git log <last-release-tag>..HEAD --pretty='%Creset- %s | [%an](https://github.com/%an)'
    ```
    - Copy the output from the command above into the top of [changelog](./CHANGELOG.md)
    - fix each `https://github.com/<author-name>` to point to the correct github username
    (the `git log` command can't do this automatically)
    - organize each commit based on their prefix into below three categories:
    ```
        ### Features
         - <a-commit-with-feat-prefix>

        ### Fixes
         - <a-commit-with-fix-prefix>

        ### Maintenance
         - <a-commit-with-maintenance-prefix>
    ```
5. Add a summary of release changes to [release notes](./RELEASE_NOTES.md)
6. Open a PR with the above, and merge that into main
7. Create new tag on merged commit with the new version (e.g. `v1.4.1`)
8. Push the tag upstream (this will kick off the release pipeline in CI)
9. Once the build for the new tag is finished, copy change log entry for newest version into draft GitHub release created as part of CI publish steps
10. Update the `appVersion` and any relevant chart changes in [helm-charts](https://github.com/honeycombio/helm-charts/tree/main/charts/refinery)
