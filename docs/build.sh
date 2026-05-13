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

set -e

cd "$(dirname "$0")"

rm -rf build
rm -rf source/_extra

# Generate Javadoc for the library module. javadoc:javadoc triggers
# generate-sources, which fetches upstream protos via wget.
(cd .. && ./mvnw -q -DskipTests -pl :datafusion-java javadoc:javadoc)

mkdir -p source/_extra
cp -R ../core/target/reports/apidocs source/_extra/api

if [ -d venv ]; then
  # shellcheck disable=SC1091
  source venv/bin/activate
fi

sphinx-build -b html -W --keep-going source build/html
