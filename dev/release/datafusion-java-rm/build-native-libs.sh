#!/bin/bash
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#

# Build the datafusion_jni release native lib inside a Linux container.
# The container's --platform determines the target arch; we just build.

set -euo pipefail

REPO=${1:-}
BRANCH=${2:-}

if [ -z "$REPO" ] || [ -z "$BRANCH" ]; then
    echo "Usage: $0 <git-repo-url> <branch-or-tag>" >&2
    exit 1
fi

echo "Building datafusion_jni for $(uname -m) from ${REPO}/${BRANCH}"

rm -rf datafusion-java
git clone "$REPO" datafusion-java
cd datafusion-java
git checkout "$BRANCH"

# Cargo writes to the workspace `rust-target/` dir (set in .cargo/config.toml),
# not the per-crate `native/target/`, so build from the repo root.
cargo build --release -p datafusion-jni

echo "Built $(pwd)/rust-target/release/libdatafusion_jni.so"
ls -l rust-target/release/libdatafusion_jni.so
