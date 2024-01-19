# Contributing guide

Thanks for your interest in contributing to PGet! We welcome contributions of all kinds, including bug reports, feature requests, documentation improvements, and code contributions.

## Running tests

To run the entire test suite:

```sh
make test
```

## Publishing a release

This project has a [GitHub Actions workflow](https://github.com/replicate/pget/blob/63220e619c6111a11952e40793ff4efed76a050e/.github/workflows/ci.yaml#L81:L81) that uses [goreleaser](https://goreleaser.com/quick-start/#quick-start) to facilitate the process of publishing new releases. The release process is triggered by manually creating and pushing a new git tag.

To publish a new release, run the following in your local checkout of pget:

```console
git checkout main
git fetch --all --tags
git tag v0.0.11
git push --tags
```

While not required, it is recommended to publish a signed tag using `git tag -s v0.0.11` (example). Pre-release tags can be created by appending a `-` and some string beyond that conforms to gorelearer's concept of semver pre-release (e.g. `-beta10`)

Then visit [github.com/replicate/pget/actions](https://github.com/replicate/pget/actions) to monitor the release process.