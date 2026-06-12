#!/usr/bin/env python3
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

"""Scaffold a new Spark bridge project from spark/scaffold/bridge-template/.

Stamps out a standalone project (Maven + Cargo) wired to the
datafusion-spark-bridge SDK: a Rust cdylib with `export_bridge!` and a demo
in-memory provider, the four Java classes (native surface, ScanBackend,
factory, DataSource shim), the DataSourceRegister service file, a shaded-jar
pom that bundles the cdylib, a pyspark smoke test, and a README with the
build/run commands.

Usage:
    python3 spark/scaffold/new_bridge.py --name acme --package com.example.acme \
        [--output DIR] [--datafusion-java REPO_ROOT]

`--name` is the Spark format short name (spark.read.format("acme")); it also
derives the class prefix (acme -> Acme, my_format -> MyFormat), the cargo
crate name, and the cdylib name. Stdlib only; no dependencies.
"""

import argparse
import re
import sys
from pathlib import Path

TEMPLATE_DIR = Path(__file__).resolve().parent / "bridge-template"


def jni_mangle(binary_class_name: str) -> str:
    """JNI symbol mangling for a class's binary name: '_' -> '_1', '.' -> '_'."""
    return binary_class_name.replace("_", "_1").replace(".", "_")


def class_prefix(name: str) -> str:
    return "".join(part.capitalize() for part in name.split("_"))


def validate(name: str, package: str) -> None:
    if not re.fullmatch(r"[a-z][a-z0-9_]*", name):
        sys.exit(f"--name must match [a-z][a-z0-9_]*, got: {name}")
    if not re.fullmatch(r"[a-z][a-z0-9_]*(\.[a-z][a-z0-9_]*)+", package):
        sys.exit(f"--package must be a dotted lowercase Java package, got: {package}")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--name", required=True, help="Spark format short name, e.g. acme")
    parser.add_argument(
        "--package", required=True, help="Java package for the bridge, e.g. com.example.acme"
    )
    parser.add_argument(
        "--output",
        help="Directory to create (default: ./<name>-spark-bridge; must not exist)",
    )
    parser.add_argument(
        "--datafusion-java",
        help="datafusion-java repo root providing the spark/bridge SDK crate "
        "(default: the repo this script lives in)",
    )
    args = parser.parse_args()

    validate(args.name, args.package)
    prefix = class_prefix(args.name)
    crate = args.name.replace("_", "-") + "-spark-bridge"
    lib = args.name + "_spark_bridge"
    repo = Path(args.datafusion_java).resolve() if args.datafusion_java else TEMPLATE_DIR.parents[2]
    sdk_path = repo / "spark" / "bridge"
    if not (sdk_path / "Cargo.toml").is_file():
        sys.exit(f"datafusion-spark-bridge crate not found at {sdk_path}")
    out = Path(args.output) if args.output else Path.cwd() / crate
    if out.exists():
        sys.exit(f"output directory already exists: {out}")

    tokens = {
        "__PKG__": args.package,
        "__PKG_PATH__": args.package.replace(".", "/"),
        "__JNI_CLASS__": jni_mangle(args.package + ".BridgeNative"),
        "__PREFIX__": prefix,
        "__FORMAT__": args.name,
        "__CRATE__": crate,
        "__LIB__": lib,
        "__BRIDGE_SDK_PATH__": str(sdk_path),
        "__DF_JAVA_VERSION__": read_repo_version(repo),
    }

    generated = []
    for src in sorted(TEMPLATE_DIR.rglob("*")):
        if not src.is_file():
            continue
        rel = str(src.relative_to(TEMPLATE_DIR))
        for token, value in tokens.items():
            rel = rel.replace(token, value)
        dst = out / rel
        dst.parent.mkdir(parents=True, exist_ok=True)
        text = src.read_text()
        for token, value in tokens.items():
            text = text.replace(token, value)
        dst.write_text(text)
        generated.append(rel)

    print(f"Generated {len(generated)} files under {out}:")
    for rel in generated:
        print(f"  {rel}")
    print()
    print("Next steps (see the generated README.md):")
    print(f"  1. cd {out}")
    print("  2. cargo build --release --manifest-path native/Cargo.toml")
    print("  3. mvn package -Dnative.profile=release")
    print(f"  4. spark.read.format(\"{args.name}\") with the shaded jar on spark.jars")


def read_repo_version(repo: Path) -> str:
    """datafusion-java's maven version, scraped from the parent pom."""
    pom = (repo / "pom.xml").read_text()
    m = re.search(r"<version>([^<]+)</version>", pom)
    if not m:
        sys.exit(f"could not find <version> in {repo}/pom.xml")
    return m.group(1)


if __name__ == "__main__":
    main()
