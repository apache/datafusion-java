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

.PHONY: all native native-runtime-metrics jvm test format clean tpch-data

all: native jvm

native:
	cd native && cargo build

# Build the native crate with the `runtime-metrics` Cargo feature enabled.
# Requires `--cfg tokio_unstable` because tokio-metrics gates its API there.
# Default `make native` does not pull this in; callers who need
# SessionContext.runtimeStats() pick this target explicitly.
native-runtime-metrics:
	cd native && RUSTFLAGS="--cfg tokio_unstable" cargo build --features runtime-metrics

jvm:
	./mvnw package -DskipTests

test: native
	./mvnw test

# Apply Java + Rust formatters in place. CI verifies the equivalent
# `:check` form inline in .github/workflows/lint.yml.
format:
	./mvnw -q spotless:apply
	cd native && cargo fmt --all

clean:
	cd native && cargo clean
	./mvnw clean

tpch-data:
	@command -v tpchgen-cli >/dev/null || \
		(echo "Install: cargo install tpchgen-cli" && exit 1)
	mkdir -p tpch-data/sf1
	tpchgen-cli -s 1 -f parquet -o tpch-data/sf1
