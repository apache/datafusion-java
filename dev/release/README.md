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

# Release Process

This guide is for maintainers creating release candidates of Apache
DataFusion Java and running the release process.

## Release Prerequisites

### Add a git remote for the `apache` repo

The instructions below assume the upstream git repo
`git@github.com:apache/datafusion-java.git` is configured as the remote
named `apache`:

```shell
git remote add apache git@github.com:apache/datafusion-java.git
```

### Add your GPG public key to the SVN `KEYS` file

If you will be releasing the final tarball, your GPG public key must be
present in:

- https://dist.apache.org/repos/dist/dev/datafusion/KEYS
- https://dist.apache.org/repos/dist/release/datafusion/KEYS

See https://infra.apache.org/release-signing.html#generate for generating
keys.

Committers can add signing keys via Subversion using their ASF account:

```shell
svn co https://dist.apache.org/repos/dist/dev/datafusion
cd datafusion
(gpg --list-sigs "Your Name" && gpg --armor --export "Your Name") >> KEYS
svn commit KEYS -m "Add key for Your Name"
```

## Release Process: Step by Step

As part of the Apache governance model, official releases consist of
signed source tarballs approved by the PMC.

### 1. Pick a Release Candidate (RC) number

Pick numbers in sequential order, with `1` for `rc1`, `2` for `rc2`, and
so on.

### 2. Create the Git tag for the Release Candidate

Release tags look like `0.1.0`, and release candidate tags look like
`0.1.0-rc1`.

```shell
git fetch apache
git tag 0.1.0-rc1 apache/main
git push apache 0.1.0-rc1
```

### 3. Create, sign, and upload the tarball

Run `create-tarball.sh` with the version and RC number:

```shell
./dev/release/create-tarball.sh 0.1.0 1
```

The script:

1. Creates and uploads the release candidate artifacts to the
   [`dist/dev/datafusion`](https://dist.apache.org/repos/dist/dev/datafusion)
   location on the Apache SVN distribution server, under
   `apache-datafusion-java-<version>-rc<rc>/`.
2. Prints an email template to send to `dev@datafusion.apache.org` for
   the release vote.

### 4. Vote on the Release Candidate

Send the email printed by the script to `dev@datafusion.apache.org`.

The vote must remain open for at least 72 hours and requires at least
three PMC `+1` votes and no `-1` votes to pass.

### Verifying Release Candidates

`dev/release/verify-release-candidate.sh` downloads the candidate
tarball, verifies its signature and checksums, and builds and tests the
source distribution:

```shell
./dev/release/verify-release-candidate.sh 0.1.0 1
```

The script requires `curl`, `git`, `gpg`, a C toolchain, `java`, and
either `shasum` or `sha256sum`/`sha512sum` to be on the `PATH`. It will
install a temporary Rust toolchain into the sandbox directory.

### 5. If the Vote Passes: Promote the Tarball

Move artifacts from the `dev` SVN area to the `release` area using
`release-tarball.sh`:

```shell
./dev/release/release-tarball.sh 0.1.0 1
```

### 6. Create the Release Git Tag

Tag the same release candidate commit with the final release tag:

```shell
git checkout 0.1.0-rc1
git tag 0.1.0
git push apache 0.1.0
```

### 7. Add the Release to Apache Reporter

Add the release at https://reporter.apache.org/addrelease.html?datafusion
so it appears in the project's board report.

### 8. Delete Old RCs and Releases

See the ASF documentation on [when to
archive](https://www.apache.org/legal/release-policy.html#when-to-archive).

Release candidates should be deleted once the release is published:

```shell
svn ls https://dist.apache.org/repos/dist/dev/datafusion | grep datafusion-java
svn delete -m "delete old DataFusion Java RC" \
  https://dist.apache.org/repos/dist/dev/datafusion/apache-datafusion-java-0.1.0-rc1/
```

Only the latest release should remain in the `release` SVN area. Delete
older releases after publishing a new one:

```shell
svn ls https://dist.apache.org/repos/dist/release/datafusion | grep datafusion-java
svn delete -m "delete old DataFusion Java release" \
  https://dist.apache.org/repos/dist/release/datafusion/datafusion-java-0.1.0
```
