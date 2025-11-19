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

# Regenerate the FlatBuffers helper files used by arrow-js. Requires a sibling
# checkout of apache/arrow (../arrow) if not provided in env and a working flatc on PATH.

set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
FORMAT_DIR="${PROJECT_ROOT}/../arrow/format"

if [[ ! -d "${FORMAT_DIR}" ]]; then
  echo "error: expected FlatBuffers schemas in ${FORMAT_DIR}" >&2
  exit 1
fi

if ! command -v flatc >/dev/null 2>&1; then
  echo "error: flatc not found on PATH" >&2
  exit 1
fi

TMPDIR="$(mktemp -d "${PROJECT_ROOT}/.flatc.XXXXXX")"
cleanup() {
  rm -rf "${TMPDIR}"
}
trap cleanup EXIT

schemas=(File Schema Message Tensor SparseTensor)

for schema in "${schemas[@]}"; do
  cp "${FORMAT_DIR}/${schema}.fbs" "${TMPDIR}/${schema}.fbs"
  sed \
    -e 's/namespace org.apache.arrow.flatbuf;//g' \
    -e 's/org\.apache\.arrow\.flatbuf\.//g' \
    "${FORMAT_DIR}/${schema}.fbs" >"${TMPDIR}/${schema}.fbs"
done

flatc --ts --ts-flat-files --ts-omit-entrypoint \
  -o "${TMPDIR}" \
  "${TMPDIR}"/{File,Schema,Message,Tensor,SparseTensor}.fbs

generated_files=(
  binary-view.ts
  list-view.ts
  large-list-view.ts
  message.ts
  record-batch.ts
  schema.ts
  type.ts
  utf8-view.ts
)

for file in "${generated_files[@]}"; do
  if [[ ! -f "${TMPDIR}/${file}" ]]; then
    echo "error: expected generated file ${file} not found" >&2
    exit 1
  fi
  install -m 0644 "${TMPDIR}/${file}" "${PROJECT_ROOT}/src/fb/${file}"
done
