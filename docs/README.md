<!--
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

# Apache DataFusion Java Documentation

This directory contains the Sphinx source for the Apache DataFusion Java
documentation site.

## Build

Building the docs requires Python 3.9 or newer. A virtual environment under
`docs/venv/` is the recommended workflow.

```sh
cd docs
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
./build.sh
```

The generated site is written to `docs/build/html/`. Open
`docs/build/html/index.html` in a browser to preview.

Subsequent builds need only:

```sh
cd docs
source venv/bin/activate
./build.sh
```

`./build.sh` runs `sphinx-build` with `-W` so warnings fail the build.
