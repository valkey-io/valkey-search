# Project Governance

Valkey Search is a sub-project of the [Valkey](https://github.com/valkey-io/valkey) project.
It aspires to the same style of governance as the core Valkey project, whose model is described in the core [GOVERNANCE.md](https://github.com/valkey-io/valkey/blob/unstable/GOVERNANCE.md).
Contributors familiar with the core project should find the roles, decision-making, and expectations here intentionally similar.

## Relationship to the Valkey project

Valkey Search is a sub-project within the Valkey project which is managed by the Valkey Technical Steering Committee (TSC).
The TSC has delegated management of this subproject to the provided list of maintainers. The TSC reserves the ability to overrule decisions made within this project, although they shall show restraint in doing so.

Our explicit goal is for Valkey Search to be **self-governing and self-contained**: day-to-day technical and project decisions should be made by this project's own maintainers, without requiring TSC involvement.
TSC interaction should be the exception (for example, escalation of an unresolved dispute or a change in the project's relationship to the broader Valkey organization), not part of the normal flow of development.

## Roles

* **Committers** are individuals with write access to the code within this repository.
* **Maintainers** are individuals with full access to the repository and own the project's management.

Both maintainers and committers are listed in [MAINTAINERS.md](MAINTAINERS.md).

## Decision making

The maintainers strive to make all decisions by consensus.
While explicit agreement of every maintainer is preferred, it is not required for consensus; rather, consensus is determined in good faith based on factors including the dominant view of the maintainers and the nature of support and objections.
When consensus cannot be reached, the maintainers decide by a simple majority vote.

Routine changes — bug fixes, compatibility improvements, and small, well-scoped extensions — follow the normal pull request review process described in [CONTRIBUTING.md](CONTRIBUTING.md).
Larger or architecturally significant changes should be raised as an issue and discussed with the community, including at one of the regular project meetings, before substantial implementation work begins.

## Maintainer composition

The core Valkey project requires that no more than one third (1/3) of the TSC members may be employees, contractors, or representatives of the same organization or affiliated organizations.
Valkey Search aspires to that same one-third per-organization limit for its maintainers.

We do not meet that goal today.
The project does not yet have enough active participants from a sufficiently diverse set of organizations to satisfy the 1/3 limit, and as a result the current maintainers are well over the desired per-company limit.
This is a known, temporary state, and reaching a healthier balance is a priority for the project.

**We are actively seeking new maintainers — particularly individuals outside of GCP and AWS — who are interested in committing the time and energy required to help lead Valkey Search.**
Developers who make significant and sustained contributions to the project will be considered for maintainership.
See [MAINTAINERS.md](MAINTAINERS.md) for the current list and [CONTRIBUTING.md](CONTRIBUTING.md) for how to get involved.

## Adding and removing maintainers

As the project matures, decisions to add or remove maintainers are made by the existing maintainers, in keeping with the practices of the core Valkey project.
Because the TSC retains ultimate authority over this sub-project, the TSC may also add or remove maintainers at its discretion.

## License of this document

This document may be used, modified, and/or distributed under the terms of the
[Creative Commons Attribution 4.0 International (CC-BY) license](https://creativecommons.org/licenses/by/4.0/legalcode).
