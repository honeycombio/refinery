# Release Process

- Check that licenses are current with `make verify-licenses`
- Regenerate documentation with `make all` from within the `tools/convert` folder.
  - If there have been changes to `rules.md`, you may need to manually modify the `rules_complete.yaml` to reflect the same change.
- If either `refinery_config.md` or `refinery_rules.md` were modified in this release, you must also open a [docs](https://github.com/honeycombio/docs) PR and update these files there under `layouts/shortcodes/subpages/refinery/`.
  - Replace the underscores (`_`) in the filenames with a dash (`-`) or the docs linter will be upset.
  - Address any feedback from the the docs team and apply that feedback back into this repo.
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

- Add a summary of release changes to [release notes](./RELEASE_NOTES.md)
- Commit changes, push, and open a release preparation pull request for review.
- Once the pull request is merged, fetch the updated `main` branch.
- Apply a tag for the new version on the merged commit (e.g. `git tag -a v1.4.1 -m "v1.4.1"`)
- Push the tag upstream (this will kick off the release pipeline in CI) e.g. `git push origin v1.4.1`
- Ensure that there is a draft GitHub release created as part of CI publish steps.
- Click "generate release notes" in GitHub for full changelog notes and any new contributors
- Publish the GitHub draft release
- Update the `appVersion` and any relevant chart changes in [helm-charts](https://github.com/honeycombio/helm-charts/tree/main/charts/refinery)
