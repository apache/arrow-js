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

set -eu

SOURCE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SOURCE_TOP_DIR="$(cd "${SOURCE_DIR}/../../" && pwd)"

if [ "$#" -ne 1 ]; then
  echo "Usage: $0 <rc>"
  echo " e.g.: $0 1"
  exit 1
fi

rc=$1

: "${RELEASE_DEFAULT:=1}"
: "${RELEASE_TAG:=${RELEASE_DEFAULT}}"
: "${RELEASE_UPLOAD:=${RELEASE_DEFAULT}}"
: "${RELEASE_CLEAN:=${RELEASE_DEFAULT}}"
: "${RELEASE_PUBLISH:=${RELEASE_DEFAULT}}"

if [ ! -f "${SOURCE_DIR}/.env" ]; then
  echo "You must create ${SOURCE_DIR}/.env"
  echo "You can use ${SOURCE_DIR}/.env.example as template"
  exit 1
fi
. "${SOURCE_DIR}/.env"

cd "${SOURCE_TOP_DIR}"

version=$(jq -r .version package.json)

git_origin_url="$(git remote get-url origin)"
repository="${git_origin_url#*github.com?}"
repository="${repository%.git}"
case "${git_origin_url}" in
git@github.com:apache/arrow-js.git | https://github.com/apache/arrow-js.git)
  : # OK
  ;;
*)
  echo "This script must be ran with working copy of apache/arrow-js."
  echo "The origin's URL: ${git_origin_url}"
  exit 1
  ;;
esac

tag="v${version}"
rc_tag="${tag}-rc${rc}"
if [ "${RELEASE_TAG}" -gt 0 ]; then
  echo "Tagging for release: ${tag}"
  git tag -a -m "${version}" "${tag}" "${rc_tag}^{}"
  git push origin "${tag}"
fi

release_id="apache-arrow-js-${version}"
source_archive="apache-arrow-js-${version}.tar.gz"
dist_url="https://dist.apache.org/repos/dist/release/arrow"
dist_base_dir="dev/release/dist"
dist_dir="${dist_base_dir}/${release_id}"
if [ "${RELEASE_UPLOAD}" -gt 0 ]; then
  echo "Checking out ${dist_url}"
  rm -rf "${dist_base_dir}"
  svn co --depth=empty "${dist_url}" "${dist_base_dir}"
  gh release download "${rc_tag}" \
    --dir "${dist_dir}" \
    --pattern "${source_archive}*" \
    --repo "${repository}" \
    --skip-existing

  echo "Uploading to release/"
  pushd "${dist_base_dir}"
  svn add "${release_id}"
  svn ci -m "Apache Arrow JS ${version}"
  popd
  rm -rf "${dist_base_dir}"
fi

if [ "${RELEASE_CLEAN}" -gt 0 ]; then
  echo "Keep only the latest versions"
  old_releases=$(
    svn ls https://dist.apache.org/repos/dist/release/arrow/ |
      grep -E '^apache-arrow-js-' |
      sort --version-sort --reverse |
      tail -n +2
  )
  for old_release_version in ${old_releases}; do
    echo "Remove old release ${old_release_version}"
    svn \
      delete \
      -m "Remove old Apache Arrow JS release: ${old_release_version}" \
      "https://dist.apache.org/repos/dist/release/arrow/${old_release_version}"
  done
fi

if [ "${RELEASE_PUBLISH}" -gt 0 ]; then
  echo "Publish to npm"
  packages_dir=packages
  rm -rf "${packages_dir}"
  mkdir -p "${packages_dir}"
  pushd "${packages_dir}"
  gh release download "${rc_tag}" \
    --dir "." \
    --pattern "*.tgz" \
    --repo "${repository}"
  read -r -p "Please enter your npm 2FA one-time password (or leave empty if you don't have 2FA enabled): " NPM_OTP </dev/tty
  for package in *.tgz; do
    npm publish "${package}" "${NPM_OTP:+--otp=${NPM_OTP}}"
  done
  rm -rf "${packages_dir}"
fi

echo
echo "Draft email for announce@apache.org, dev@arrow.apache.org"
echo "and user@arrow.apache.org mailing lists"
echo ""
echo "---------------------------------------------------------"
cat <<MAIL
To: announce@apache.org
Cc: dev@arrow.apache.org, user@arrow.apache.org
Subject: [ANNOUNCE] Apache Arrow JS ${version} released

The Apache Arrow community is pleased to announce the Apache Arrow JS
${version} release.

The release is available now.

Source archive:
  https://www.apache.org/dyn/closer.lua/arrow/apache-arrow-js-${version}/

On www.npmjs.com:
  https://www.npmjs.com/package/apache-arrow
  https://www.npmjs.com/org/apache-arrow

Read the full changelog:
  https://github.com/apache/arrow-js/releases/tag/${tag}

What is Apache Arrow?
---------------------

Apache Arrow is a universal columnar format and multi-language toolbox
for fast data interchange and in-memory analytics. It houses a set of
canonical in-memory representations of flat and hierarchical data
along with multiple language-bindings for structure manipulation. It
also provides low-overhead streaming and batch messaging, zero-copy
interprocess communication (IPC), and vectorized in-memory analytics
libraries.

Please report any feedback to the GitHub repository:
  https://github.com/apache/arrow-js/issues
  https://github.com/apache/arrow-js/discussions

Regards,
The Apache Arrow community.
MAIL
echo "---------------------------------------------------------"
echo
echo "Success! The release is available here:"
echo "  https://dist.apache.org/repos/dist/release/arrow/${release_id}"
echo "  https://www.npmjs.com/package/apache-arrow"
echo "  https://www.npmjs.com/org/apache-arrow"
echo
echo "Add this release to ASF's report database:"
echo "  https://reporter.apache.org/addrelease.html?arrow"
