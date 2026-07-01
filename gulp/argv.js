// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

import { parseArgs } from 'node:util';

const options = {
    all: { type: 'boolean' },
    verbose: { type: 'boolean', short: 'v' },
    target: { type: 'string', default: '' },
    module: { type: 'string', default: '' },
    coverage: { type: 'boolean', default: false },
    tests: { type: 'string', multiple: true, default: ['test/unit/'] },
    targets: { type: 'string', short: 't', multiple: true, default: [] },
    modules: { type: 'string', short: 'm', multiple: true, default: [] },
};

const { values, tokens } = parseArgs({
    options,
    args: process.argv.slice(2),
    strict: false,
    allowPositionals: true,
    tokens: true,
});

// Rebuild the `_unknown` array that command-line-args' `{ partial: true }` mode
// used to produce. gulp/test-task.js forwards _unknown to Jest, so any flag or
// positional we don't recognise here (e.g. `--testNamePattern foo`, `--bail`)
// needs to round-trip through with its original raw form preserved. parseArgs
// only flags positionals as such; unknown options land in `values` and need to
// be reconstructed from the token stream.
const knownNames = new Set(Object.keys(options));
const unknown = [];
for (const tok of tokens) {
    if (tok.kind === 'positional') {
        unknown.push(tok.value);
    } else if (tok.kind === 'option' && !knownNames.has(tok.name)) {
        unknown.push(tok.rawName);
        // `--foo=bar` parses to a single token with `inlineValue: true`; the
        // separate-arg form `--foo bar` puts `bar` in a following positional
        // token, which we'll pick up on the next loop iteration.
        if (tok.inlineValue) unknown.push(tok.value);
        delete values[tok.name];
    }
}
values._unknown = unknown;

export const argv = values;
export const { targets, modules } = argv;

if (argv.target === `src`) {
    argv.target && !targets.length && targets.push(argv.target);
} else {
    argv.target && !targets.length && targets.push(argv.target);
    argv.module && !modules.length && modules.push(argv.module);
    (argv.all || !targets.length) && targets.push(`all`);
    (argv.all || !modules.length) && modules.push(`all`);
}
