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

"""Sphinx configuration for the Apache DataFusion Java documentation."""

project = "Apache DataFusion Java"
copyright = "2026, Apache Software Foundation"
author = "Apache Software Foundation"

extensions = [
    "sphinx.ext.mathjax",
    "sphinx.ext.napoleon",
    "myst_parser",
]

source_suffix = {
    ".md": "markdown",
}

templates_path = ["_templates"]
exclude_patterns = []

html_theme = "pydata_sphinx_theme"
html_theme_options = {
    "use_edit_page_button": False,
    "show_toc_level": 2,
}

html_context = {
    "github_user": "apache",
    "github_repo": "datafusion-java",
    "github_version": "main",
    "doc_path": "docs/source",
}

html_static_path = ["_static"]

# Auto-generate anchor links for headings h1, h2, h3.
myst_heading_anchors = 3

# Enable nice rendering of GitHub-style task lists.
myst_enable_extensions = ["tasklist"]
