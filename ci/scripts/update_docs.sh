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

version="${1}"
target_branch="${2}"

html_escape() {
  # & -> &amp; must be the first substitution
  sed -e "s/&/&amp;/g" \
    -e "s/</&lt;/g" \
    -e "s/>/&gt;/g" \
    -e "s/\"/&quot;/g" \
    -e "s/'/&apos;/g"
}

if ! git fetch origin "${target_branch}"; then
  git worktree add --orphan -b "${target_branch}" site
else
  git worktree add site "origin/${target_branch}"
fi

tar_gz="${PWD}/apache-arrow-js-docs-${version}.tar.gz"

extract_docs() {
  local destination="${1}"

  rm -rf "${destination}"
  mkdir -p "${destination}"
  pushd "${destination}"
  tar xf "${tar_gz}" --strip-components=1
  popd
  git add "${destination}"
}

pushd site
if [ "${target_branch}" = "asf-site" ]; then
  # Update https://arrow.apache.org/js/main/
  extract_docs main

  # Create .htaccess
  cat >.htaccess <<HTACCESS
RedirectMatch "^/js/$" "/js/current/"
HTACCESS
  git add .htaccess
else
  # Remove data for nonexistent branches
  for branch in *; do
    if [ ! -d "${branch}" ]; then
      continue
    fi
    if ! git fetch origin "${branch}"; then
      git rm "${branch}"
    fi
  done

  # Update the pushed branch
  extract_docs "${GITHUB_REF_NAME}"

  # Create index.html
  {
    echo "<!DOCTYPE html>"
    echo "<html>"
    echo "  <head>"
    echo "    <title>Apache Arrow JS documents</title>"
    echo "  </head>"
    echo "  <body>"
    echo "    <ul>"
    for branch in *; do
      if [ ! -d "${branch}" ]; then
        continue
      fi
      escaped_branch="$(echo "${branch}" | html_escape)"
      echo "      <li>"
      echo "        <a href=\"${escaped_branch}/\">${escaped_branch}</a>"
      echo "      </li>"
    done
    echo "    </ul>"
    echo "  </body>"
    echo "</html>"
  } >index.html
  git add index.html
fi
popd
