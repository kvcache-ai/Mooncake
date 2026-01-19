# Governance

This document describes the governance model and code maintenance process for the Mooncake project.

Mooncake is an open-source project that follows a community-driven governance model. The project is maintained by a group of Collaborators who oversee the codebase and ensure code quality, while Contributors from the community help improve the project through code contributions, bug reports, documentation, and other valuable inputs.

## Collaborator

Collaborators are trusted members of the community who have been granted specific permissions to review, approve, and merge code changes. This model helps protect critical code paths while enabling efficient development workflows. Collaborators may hold one or more of the following roles:

- **Codeowner**: Codeowners are maintainers whose primary responsibility is to protect critical code. Each pull request needs at least one Codeowner approval if it modifies files protected by [CODEOWNERS](https://github.com/kvcache-ai/Mooncake/blob/main/.github/CODEOWNERS). When a pull request is submitted, Codeowners are responsible for reviewing the code in a timely manner, or delegating the review to appropriate reviewers when unavailable, and ultimately approving the pull request. This role is not just an honor but a significant responsibility, as pull requests cannot be merged without Codeowner approval. Current Codeowners are listed in the [CODEOWNERS](https://github.com/kvcache-ai/Mooncake/blob/main/.github/CODEOWNERS) file.

- **Write**: Members with Write permission are trusted contributors responsible for reviewing code in a timely manner and ensuring code quality. They actively participate in code reviews, providing feedback on code correctness, functionality, style adherence, test coverage, and documentation. After a Codeowner has approved a pull request and all required tests have passed, Write members have the permission to merge the pull request. Their role is crucial in maintaining both code quality and the project's development velocity.

## Development Process

### Technical Discussion

We encourage developers to initiate technical discussions in the community before starting formal development. This helps gather valuable feedback from other contributors and users, ensuring that proposed changes align with the project's goals and benefit the broader community.

- **Major Changes**: For significant architectural changes (typically >500 LOC excluding tests), an RFC (Request for Comments) must be submitted on GitHub to solicit comprehensive feedback from the community. This is a mandatory step to ensure all stakeholders have an opportunity to provide input and raise concerns before implementation begins.

- **Urgent Changes**: For urgent modifications or critical bug fixes that require immediate attention, developers can raise the issue on [Slack](https://join.slack.com/t/mooncake-project/shared_invite/zt-3ig4fjai8-KH1zIm3x8Vm8WqyH0i_JaA) and request relevant collaborators to participate quickly. This allows for rapid response while still maintaining communication with the team.

- **Breaking Changes**: Any changes that break backward compatibility require broader consensus and must be clearly documented with migration guides. These should always go through the RFC process to ensure all implications are thoroughly discussed.

### Pull Request Merge Process

The pull request merge process ensures code quality through collaborative review and automated checks:

1. **PR Submission**: Contributors submit pull requests with clear descriptions following the [PR template](https://github.com/kvcache-ai/Mooncake/blob/main/.github/pull_request_template.md). The description should explain the changes, their motivation, and any relevant context.

2. **Automatic Review Requests**: Once a PR is submitted, GitHub will automatically request reviews from Codeowners based on the CODEOWNERS file. Additionally, an AI assistant will automatically review the code to provide initial feedback and catch common issues.

3. **Code Review**: Codeowners conduct code reviews or invite appropriate reviewers with relevant expertise. Additionally, anyone in the community who is relevant or interested is welcome to help with reviews. Contributors should address all review comments and feedback.

4. **Approval and Merge**: Once approved by at least one Codeowner and all required tests have passed, a Write member can merge the pull request. The PR must meet all quality standards and have no blocking issues before merging.

If you encounter any issues during the merge process, you can discuss them on [Slack](https://join.slack.com/t/mooncake-project/shared_invite/zt-3ig4fjai8-KH1zIm3x8Vm8WqyH0i_JaA) to get help from Collaborators.

---

*This governance model is designed to be flexible and may evolve as the project grows. We welcome feedback and suggestions for improvement.*
