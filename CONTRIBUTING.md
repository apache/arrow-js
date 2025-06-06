<!---
  Licensed to the Apache Software Foundation (ASF) under one
  or more contributor license agreements.  See the NOTICE file
  distributed with this work for additional information
  regarding copyright ownership.  The ASF licenses this file
  to you under the Apache License, Version 2.0 (the
  "License"); you may not use this file except in compliance
  with the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing,
  software distributed under the License is distributed on an
  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  KIND, either express or implied.  See the License for the
  specific language governing permissions and limitations
  under the License.
-->

# How to contribute to Apache Arrow JavaScript

We utilize [Conventional
Commits](https://www.conventionalcommits.org/en/v1.0.0/) for our
commit messages. This helps maintain the semantic versioning of this
module.

Please use the following commit types: `chore`, `feat`, `fix`.

If a PR includes any breaking change, please add `!` such as `feat!`
and `fix!`.

We will use these commit types to determine the next version. If we
have only `fix` (and `chore`) changes, we will increase the patch
version. If we have any `feat` change, we will increase the minor
version. If we have any `feat!` or `fix!` change, we will increase the
major version.

For example:

```
fix: Handle empty structs in C data interface

fix!: Fix performance regression with API change

chore: Update CI environment

feat: Support new encoding type

feat!: Reconstruct API
```

## Did you find a bug?

The Arrow JavaScript project uses GitHub as a bug tracker. To report a
bug, sign in to your GitHub account, navigate to [GitHub
issues](https://github.com/apache/arrow-js/issues) and click on **New
issue** .

Before you create a new bug entry, we recommend you first search among
existing issues in [GitHub](https://github.com/apache/arrow-js/issues).

## Did you write a patch that fixes a bug or brings an improvement?

- Create a GitHub issue and submit your changes as a GitHub Pull Request.
- [Reference the issue](https://docs.github.com/en/issues/tracking-your-work-with-issues/using-issues/linking-a-pull-request-to-an-issue#linking-a-pull-request-to-an-issue-using-a-keyword) in your PR description.
- Add the PR title. The PR title will be used as the eventual commit message, so please make it descriptive but succinct.

Example #1:

```
chore: Document the pull request process

## What's Changed

Explain how to open a pull request and what the title, body, and labels should be.

Closes #12345.
```

Example #2:

```
feat: Expose Netty server builder in Flight

## What's Changed

Allow direct usage of gRPC APIs for low-level control.

Closes #42424.
```

### Minor fixes

Any functionality change should have a GitHub issue opened. For minor
changes that affect documentation, you do not need to open up a GitHub
issue. If your changes meet one of the following, they're minor
changes:

*  Grammar, usage and spelling fixes that affect no more than 2 files
*  Documentation updates affecting no more than 2 files and not more
   than 500 words.

## Do you want to propose a significant new feature or an important refactoring?

We ask that all discussions about major changes in the codebase happen
publicly on the GitHub issues or [arrow-dev
mailing-list](https://lists.apache.org/list.html?dev@arrow.apache.org).

## Do you have questions about the source code, the build procedure or the development process?

You can also ask on the [arrow-dev
mailing-list](https://lists.apache.org/list.html?dev@arrow.apache.org)
or [GitHub
Discussions](https://github.com/apache/arrow-js/discussions).

## Further information

Please read our [development
documentation](https://arrow.apache.org/docs/developers/index.html) or
look through the [New Contributor's
Guide](https://arrow.apache.org/docs/developers/guide/index.html).
