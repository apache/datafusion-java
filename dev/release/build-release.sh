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

# Build a multi-platform datafusion-java JAR bundling native libs for
# linux/amd64, linux/aarch64, darwin/x86_64, and darwin/aarch64. The
# resulting JAR is installed into a temporary local Maven repository
# whose path is printed at the end. This script must run on a macOS host.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" >/dev/null && pwd)"
PROJECT_HOME="$(cd "${SCRIPT_DIR}/../.." >/dev/null && pwd)"

REPO="https://github.com/apache/datafusion-java.git"
BRANCH="main"
IMGTAG="latest"

function usage {
    cat <<EOF
Usage: $(basename "$0") [-r repo] [-b branch] [-t tag]

  -r repo    git repo URL to clone inside the linux build containers
             (default: ${REPO})
  -b branch  git branch or tag to check out inside the containers
             (default: ${BRANCH})
  -t tag     docker image tag for the two builder images
             (default: ${IMGTAG})

Produces a multi-platform datafusion-java JAR in a temporary local Maven
repo; the repo path is printed at the end. The host must be macOS.
EOF
    exit 1
}

while getopts "r:b:t:h" opt; do
    case "$opt" in
        r) REPO="$OPTARG" ;;
        b) BRANCH="$OPTARG" ;;
        t) IMGTAG="$OPTARG" ;;
        h|*) usage ;;
    esac
done

if [ "$(uname -s)" != "Darwin" ]; then
    echo "This script must run on a macOS host (got: $(uname -s))." >&2
    exit 1
fi

JAVA_VERSION=$(java -version 2>&1 | awk -F '"' '/version/ {print $2}' | awk -F '.' '{print $1}')
if [ -z "$JAVA_VERSION" ] || [ "$JAVA_VERSION" -lt 17 ]; then
    echo "Java 17+ is required. Found: $(java -version 2>&1 | head -n 1)" >&2
    exit 1
fi

HOST_ARCH="$(uname -m)"  # arm64 (Apple Silicon) or x86_64 (Intel)

case "$HOST_ARCH" in
    arm64)
        HOST_DARWIN_DIR="aarch64"
        OTHER_DARWIN_TARGET="x86_64-apple-darwin"
        OTHER_DARWIN_DIR="x86_64"
        ;;
    x86_64)
        HOST_DARWIN_DIR="x86_64"
        OTHER_DARWIN_TARGET="aarch64-apple-darwin"
        OTHER_DARWIN_DIR="aarch64"
        ;;
    *)
        echo "Unsupported macOS arch: $HOST_ARCH" >&2
        exit 1
        ;;
esac

CONTAINER_AMD64="datafusion-java-amd64-builder-container"
CONTAINER_ARM64="datafusion-java-arm64-builder-container"
IMAGE_AMD64="datafusion-java-rm-amd64:${IMGTAG}"
IMAGE_ARM64="datafusion-java-rm-arm64:${IMGTAG}"

CLEANUP=1
cleanup() {
    [ "$CLEANUP" != "0" ] || return 0
    echo "Cleaning up build containers..."
    docker rm -f "$CONTAINER_AMD64" "$CONTAINER_ARM64" >/dev/null 2>&1 || true
    CLEANUP=0
}
trap cleanup SIGINT SIGTERM EXIT

echo "Cleaning previous Java and native build output"
(cd "$PROJECT_HOME" && ./mvnw -q clean)
(cd "$PROJECT_HOME/native" && cargo clean)

echo "Building amd64 builder image"
docker build --no-cache \
    --platform=linux/amd64 \
    -t "$IMAGE_AMD64" \
    "$SCRIPT_DIR/datafusion-java-rm"

echo "Building arm64 builder image"
docker build --no-cache \
    --platform=linux/arm64 \
    -t "$IMAGE_ARM64" \
    "$SCRIPT_DIR/datafusion-java-rm"

echo "Building linux/amd64 native lib"
docker run --name "$CONTAINER_AMD64" \
    --platform=linux/amd64 \
    "$IMAGE_AMD64" "$REPO" "$BRANCH"

echo "Building linux/aarch64 native lib"
docker run --name "$CONTAINER_ARM64" \
    --platform=linux/arm64 \
    "$IMAGE_ARM64" "$REPO" "$BRANCH"

JVM_TARGET_DIR="$PROJECT_HOME/core/target/classes/org/apache/datafusion"

mkdir -p "$JVM_TARGET_DIR/linux/amd64"
docker cp \
    "$CONTAINER_AMD64:/opt/datafusion-java-rm/datafusion-java/native/target/release/libdatafusion_jni.so" \
    "$JVM_TARGET_DIR/linux/amd64/"

mkdir -p "$JVM_TARGET_DIR/linux/aarch64"
docker cp \
    "$CONTAINER_ARM64:/opt/datafusion-java-rm/datafusion-java/native/target/release/libdatafusion_jni.so" \
    "$JVM_TARGET_DIR/linux/aarch64/"

echo "Building macOS native libs on the host (host=$HOST_ARCH)"
rustup target add "$OTHER_DARWIN_TARGET"

(cd "$PROJECT_HOME/native" && cargo build --release)
(cd "$PROJECT_HOME/native" && cargo build --release --target "$OTHER_DARWIN_TARGET")

mkdir -p "$JVM_TARGET_DIR/darwin/$HOST_DARWIN_DIR"
cp "$PROJECT_HOME/native/target/release/libdatafusion_jni.dylib" \
   "$JVM_TARGET_DIR/darwin/$HOST_DARWIN_DIR/"

mkdir -p "$JVM_TARGET_DIR/darwin/$OTHER_DARWIN_DIR"
cp "$PROJECT_HOME/native/target/$OTHER_DARWIN_TARGET/release/libdatafusion_jni.dylib" \
   "$JVM_TARGET_DIR/darwin/$OTHER_DARWIN_DIR/"

echo "Installing JAR into local Maven repo"
LOCAL_REPO=$(mktemp -d /tmp/datafusion-java-staging-repo-XXXXXX)
(cd "$PROJECT_HOME" && ./mvnw "-Dmaven.repo.local=$LOCAL_REPO" -DskipTests install)

echo ""
echo "===================================================================="
echo "Multi-platform JAR installed to local Maven repo: $LOCAL_REPO"
JAR_PATH=$(find "$LOCAL_REPO/org/apache/datafusion/datafusion-java" -name 'datafusion-java-*.jar' \
             -not -name '*-sources.jar' -not -name '*-javadoc.jar' | head -n 1)
echo "JAR: $JAR_PATH"
echo "Bundled native libraries:"
unzip -l "$JAR_PATH" | grep -E 'libdatafusion_jni\.(so|dylib)$' || true
echo "===================================================================="
