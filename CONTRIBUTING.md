# Contributing to Mooncake


Thank you for your interest in contributing to Mooncake! Our community warmly welcomes everyone and values all contributions, whether big or small. Motivated by the [contribution guidelines](https://docs.vllm.ai/en/latest/contributing/overview.html) in the vLLM community, here are several ways you can get involved in the project:

- Identify and report any issues or bugs.
- Request or add support for a new component of Mooncake (such as a new transport class).
- Suggest or implement new features.
- Improve documentation or contribute a how-to guide.
- More unit tests and evaluation scripts.


# Contribution Guidelines

## Pull Requests & Code Reviews

### PR Title and Classification

Use a prefixed PR title to indicate the type of changes. Please use one of the following:

- ``[Bugfix]`` for bug fixes.
- ``[CI/Build]`` for build or continuous integration improvements.
- ``[Doc]`` for documentation fixes and improvements.
- ``[Integration]`` for changes in the ``mooncake-integration``.
- ``[P2PStore]`` for changes in the ``mooncake-p2p-store``.
- ``[Store]`` for changes in the ``mooncake-store``.
- ``[TransferEngine]`` for changes in the ``mooncake-transfer-engine``.
- ``[Misc]`` for PRs that do not fit the above categories. Please use this
  sparingly.

### RFC Discussion

For major architectural changes (>500 LOC excluding tests), we would expect a GitHub issue (RFC) discussing the technical design and justification.

## Code Quality

The PR needs to meet the following code quality standards:

- We adhere to [Google Python style guide](https://google.github.io/styleguide/pyguide.html) and [Google C++ style guide](https://google.github.io/styleguide/cppguide.html).
- The code needs to be well-documented to ensure future contributors can easily understand the code.
- Include sufficient tests to ensure the project stays correct and robust. This includes both unit tests and integration tests.
- Please add documentation to ``doc/`` if the PR modifies the user-facing behaviors of Mooncake. It helps Mooncake users understand and utilize the new features or changes.


**Finally, thank you for taking the time to read these guidelines and for your interest in contributing to Mooncake.
All of your contributions help make Mooncake a great tool and community for everyone!**
