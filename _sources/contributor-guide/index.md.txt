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

# Contributor Guide

Bug reports, design discussion, and patches are welcome. This project follows
the Apache DataFusion contribution model.

## Filing issues and discussing changes

- File bugs and feature requests on
  [GitHub issues](https://github.com/apache/datafusion-java/issues).
- For larger or design-level discussion, the mailing list is
  [dev@datafusion.apache.org](mailto:dev@datafusion.apache.org).
- Please open an issue before sending a PR for any significant change so
  the approach can be agreed on first.

## Development workflow

Branch from `main`, write changes with
[conventional commit](https://www.conventionalcommits.org/) messages in
the imperative mood (e.g. `feat: add foo`, `fix(native): handle bar`),
and open a pull request targeting `main`.

```{toctree}
:maxdepth: 1

development
code-style
releasing
updating-datafusion-version
```
