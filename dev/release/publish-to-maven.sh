#!/bin/bash
#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

###
# Based on Apache DataFusion Comet's publish-to-maven.sh (itself adapted
# from Spark's release-build.sh).
###

function usage {
    local NAME=$(basename "$0")
    cat << EOF
usage: $NAME options

Publish signed artifacts to Apache Nexus staging.

Options:
  -u ASF_USERNAME   Username of ASF committer account
  -r LOCAL_REPO     Path to the local Maven repo created by build-release.sh

The following will be prompted for:
  ASF_PASSWORD      Password of ASF committer account
  GPG_KEY           Optional: specific GPG key id to sign with
  GPG_PASSPHRASE    Passphrase for the GPG signing key
EOF
    exit 1
}

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" >/dev/null && pwd)"
PROJECT_HOME="$(cd "${SCRIPT_DIR}/../.." >/dev/null && pwd)"

ASF_USERNAME=""
LOCAL_REPO=""

NEXUS_ROOT=https://repository.apache.org/service/local/staging
NEXUS_PROFILE=789e15c00fd47

while getopts "u:r:h" opt; do
    case "$opt" in
        u) ASF_USERNAME="$OPTARG" ;;
        r) LOCAL_REPO="$OPTARG" ;;
        h|*) usage ;;
    esac
done

if [ -z "$LOCAL_REPO" ]; then
    echo "Please provide the local Maven repo path (-r), as produced by build-release.sh." >&2
    usage
fi

if [ -z "$ASF_USERNAME" ]; then
    read -p "ASF Username : " ASF_USERNAME && echo ""
fi
read -s -p "ASF Password : " ASF_PASSWORD && echo ""
read -s -p "GPG Key (optional) : " GPG_KEY && echo ""
read -s -p "GPG Passphrase : " GPG_PASSPHRASE && echo ""

if [ -z "$ASF_USERNAME" ] || [ -z "$ASF_PASSWORD" ] || [ -z "$GPG_PASSPHRASE" ]; then
    echo "Missing credentials" >&2
    exit 1
fi

GPG="gpg --pinentry-mode loopback"
if [ -n "$GPG_KEY" ]; then
    GPG="$GPG -u $GPG_KEY"
fi

SHA1SUM=$(which sha1sum || which shasum)

GIT_HASH=$(cd "$PROJECT_HOME" && git rev-parse --short HEAD)

echo "Creating Nexus staging repository"
REPO_REQUEST="<promoteRequest><data><description>Apache DataFusion Java (commit $GIT_HASH)</description></data></promoteRequest>"
REPO_REQUEST_RESPONSE=$(curl -X POST -d "$REPO_REQUEST" -u "$ASF_USERNAME:$ASF_PASSWORD" \
    -H "Content-Type:application/xml" \
    "$NEXUS_ROOT/profiles/$NEXUS_PROFILE/start")

STAGED_REPO_ID=$(echo "$REPO_REQUEST_RESPONSE" | xmllint --xpath "//stagedRepositoryId/text()" -)
echo "Created Nexus staging repository: $STAGED_REPO_ID"

if [ -z "$STAGED_REPO_ID" ]; then
    echo "Error creating staged repository" >&2
    echo "$REPO_REQUEST_RESPONSE" >&2
    exit 1
fi

echo "Deploying artifacts from $LOCAL_REPO"
pushd "$LOCAL_REPO/org/apache/datafusion" >/dev/null

# Remove any extra files that mvn install might have written alongside
# the jar/pom (e.g. -lastUpdated metadata files).
find . -type f | grep -v '\.jar$' | grep -v '\.pom$' | xargs rm -f || true

echo "Creating hash and signature files"
for file in $(find . -type f); do
    echo "$GPG_PASSPHRASE" | $GPG --passphrase-fd 0 --output "$file.asc" \
        --detach-sig --armour "$file"
    if command -v md5 >/dev/null; then
        md5 -q "$file" > "$file.md5"
    else
        md5sum "$file" | cut -f1 -d' ' > "$file.md5"
    fi
    $SHA1SUM "$file" | cut -f1 -d' ' > "$file.sha1"
done

NEXUS_UPLOAD=$NEXUS_ROOT/deployByRepositoryId/$STAGED_REPO_ID
echo "Uploading files to $NEXUS_UPLOAD"
for file in $(find . -type f); do
    FILE_SHORT=$(echo "$file" | sed -e "s/\.\///")
    DEST_URL="$NEXUS_UPLOAD/org/apache/datafusion/$FILE_SHORT"
    echo "  Uploading $FILE_SHORT"
    curl --fail-with-body \
        -u "$ASF_USERNAME:$ASF_PASSWORD" \
        --upload-file "$FILE_SHORT" "$DEST_URL"
done

echo "Closing nexus staging repository"
REPO_REQUEST="<promoteRequest><data><stagedRepositoryId>$STAGED_REPO_ID</stagedRepositoryId><description>Apache DataFusion Java (commit $GIT_HASH)</description></data></promoteRequest>"
curl --fail-with-body -X POST -d "$REPO_REQUEST" -u "$ASF_USERNAME:$ASF_PASSWORD" \
    -H "Content-Type:application/xml" \
    "$NEXUS_ROOT/profiles/$NEXUS_PROFILE/finish"
echo "Closed Nexus staging repository: $STAGED_REPO_ID"

popd >/dev/null
