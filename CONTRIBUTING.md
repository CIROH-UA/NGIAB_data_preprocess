# How to contribute

Anyone may contribute. You do not need to be affiliated with CIROH or any partner institution. The expectations below exist to keep the projects reproducible and reviewable, not to gatekeep.

## Ways to contribute

File an issue (bugs, feature ideas, questions), open a pull request (code, configuration, tests, documentation), participate in GitHub Discussions, or improve the training and product docs.

## Standard pull request workflow

- Open or comment on an issue describing the change, so effort is not duplicated.
- Fork the repository (or branch, if you have write access). Name branches descriptively.
- Make focused changes; keep pull requests as small as practical. Signed commits are recommended.
- Add or update tests where applicable, and update documentation and any changelog the repository maintains.
- State whether the change affects model behavior, input-data assumptions, configuration, calibration, reproducibility, performance, or output interpretation, so the right reviewer is engaged.
- Open a pull request against the default branch, complete the PR template if present, and reference the related issue.
- Pass automated checks (CI, linting, tests). A maintainer reviews; address feedback.

## Contribution standards

Contributions are expected to be your own work (or properly attributed and compatibly licensed), to build and pass tests, to follow each repository's coding conventions, to include reasonable documentation, and to respect the Code of Conduct. Maintainers may request changes or, with explanation, decline contributions that fall outside project scope or that introduce incompatible licenses or unverifiable model artifacts.

## Recognition

Contributions are recognized through Git history, release notes, and citation metadata. The project values non-code contributions — documentation, evaluation, issue triage, mentoring — as first-class.

## Maintaining contributed models and components

Contributing a model, model configuration, or other substantial, separately identifiable component to NGIAB carries an ongoing responsibility: the contributor — or the institution or owner on whose behalf it was contributed — is expected to maintain it after integration. Maintenance means keeping the contribution working as the ecosystem evolves: updating it for changes in the NextGen framework, the hydrofabric, forcing formats, and the surrounding NGIAB/NRDS tooling; fixing defects attributable to the contribution; and responding to issues filed against it within a reasonable time. This responsibility attaches to substantial contributions (a BMI model, a standing NRDS configuration), not to one-off fixes to code that others maintain.

Because everything here is open source, maintenance can move: an owner who can no longer maintain a contribution may hand it off to another willing maintainer, the community may adopt it, or — if no one steps forward — it may be deprecated. The project does not guarantee to maintain contributed models on an owner's behalf.

**Unmaintained contributions.** If a contributed model or configuration breaks (for instance, after a framework or hydrofabric update) and the responsible owner is unresponsive for 30 days after a good-faith attempt to reach them, the maintainers may deprecate it or remove it from NGIAB, with notice and preservation of the public record.