#!/usr/bin/env python3
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

import json
import os
import uuid
import logging
from pathlib import Path
from typing import List

from benchadapt import BenchmarkResult
from benchadapt.adapters import BenchmarkAdapter
from benchadapt.log import log

log.setLevel(logging.DEBUG)

ARROW_ROOT = Path(__file__).parent.parent.parent.resolve()
SCRIPTS_PATH = ARROW_ROOT / "ci" / "scripts"

# `github_commit_info` is meant to communicate GitHub-flavored commit
# information to Conbench. See
# https://github.com/conbench/conbench/blob/cf7931f/benchadapt/python/benchadapt/result.py#L66
# for a specification.
github_commit_info = {"repository": "https://github.com/apache/arrow-js"}

if os.environ.get("CONBENCH_REF") == "main":
    # Assume GitHub Actions CI. The environment variable lookups below are
    # expected to fail when not running in GitHub Actions.
    github_commit_info = {
        "repository": f'{os.environ["GITHUB_SERVER_URL"]}/{os.environ["GITHUB_REPOSITORY"]}',
        "commit": os.environ["GITHUB_SHA"],
        "pr_number": None,  # implying default branch
    }
    run_reason = "commit"
else:
    # Local dev environment. Do not include commit information since this is
    # not a controlled CI environment.
    # Allow user to optionally inject a custom piece of information into the
    # run reason via environment.
    run_reason = "localdev"
    custom_reason_suffix = os.getenv("CONBENCH_CUSTOM_RUN_REASON")
    if custom_reason_suffix is not None:
        run_reason += f" {custom_reason_suffix.strip()}"


class JSAdapter(BenchmarkAdapter):
    # bench.sh writes bench_stats.json into the calling directory (repo root)
    result_file = str(ARROW_ROOT / "bench_stats.json")
    command = ["bash", str(SCRIPTS_PATH / "bench.sh"), str(ARROW_ROOT), "--json"]

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(command=self.command, *args, **kwargs)

    def _transform_results(self) -> List[BenchmarkResult]:
        with open(self.result_file, "r") as f:
            raw_results = json.load(f)

        run_id = uuid.uuid4().hex

        # Group results by suite so each suite shares a batch_id
        suite_batch_ids: dict = {}

        parsed_results = []
        for result in raw_results:
            suite = result.get("suite", "unknown")
            if suite not in suite_batch_ids:
                suite_batch_ids[suite] = uuid.uuid4().hex
            batch_id = suite_batch_ids[suite]

            # benny reports:
            #   ops            - operations per second
            #   details.median - median time per operation, in seconds
            #   samples        - number of samples collected
            parsed = BenchmarkResult(
                run_id=run_id,
                batch_id=batch_id,
                stats={
                    "data": [result["ops"]],
                    "unit": "i/s",
                    "times": [result["details"]["median"]],
                    "time_unit": "s",
                    "iterations": result["samples"],
                },
                context={
                    "benchmark_language": "JavaScript",
                },
                tags={
                    "suite": suite,
                    "name": result["name"],
                },
                run_reason=run_reason,
                github=github_commit_info,
            )
            parsed.run_name = (
                f"{parsed.run_reason}: {github_commit_info.get('commit')}"
            )
            parsed_results.append(parsed)

        return parsed_results


if __name__ == "__main__":
    js_adapter = JSAdapter(result_fields_override={"info": {}})
    js_adapter()
