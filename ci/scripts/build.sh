#!/usr/bin/env bash
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

set -exu

source_dir="${1}"

: "${BUILD_DOCS_JS:=OFF}"

pushd "${source_dir}"

yarn --immutable
yarn build

if [ "${BUILD_DOCS_JS}" = "ON" ]; then
  # If upstream is defined, use it as remote.
  # Otherwise use origin which could be a fork on PRs.
  if [[ "$(git config --get remote.upstream.url)" =~ "https://github.com/apache/arrow-js" ]]; then
    yarn doc --gitRemote upstream
  elif [[ "$(basename -s .git $(git config --get remote.origin.url))" == "arrow-js" ]]; then
    yarn doc
  else
    echo "Failed to build docs because the remote is not set correctly. Please set the origin or upstream remote to https://github.com/apache/arrow-js.git."
    exit 0
  fi
fi

popd
