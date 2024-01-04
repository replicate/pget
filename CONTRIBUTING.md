# Contributing guide

Thanks for your interest in contributing to PGet! We welcome contributions of all kinds, including bug reports, feature requests, documentation improvements, and code contributions.

## Running tests

To run the entire test suite:

```sh
make test
```

## Publishing a release

This project has a [GitHub Actions workflow](https://github.com/replicate/pget/blob/63220e619c6111a11952e40793ff4efed76a050e/.github/workflows/ci.yaml#L81:L81) that uses [goreleaser](https://goreleaser.com/quick-start/#quick-start) to facilitate the process of publishing new releases. The release process is triggered by manually creating and pushing a new git tag.

To publish a new release, run the following in your local checkout of cog:

```console
git checkout main
git fetch --all --tags
git tag v0.0.11
git push --tags
```

Then visit [github.com/replicate/pget/actions](https://github.com/replicate/pget/actions) to monitor the release process.