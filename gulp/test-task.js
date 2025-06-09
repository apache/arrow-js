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

import { deleteAsync as del } from 'del';
import path from 'node:path';
import { mkdirp } from 'mkdirp';
import { argv } from './argv.js';
import { promisify } from 'node:util';
import { glob } from 'glob';
import child_process from 'node:child_process';
import { memoizeTask } from './memoize-task.js';
import fs from 'node:fs';
const readFile = promisify(fs.readFile);
import asyncDoneSync from 'async-done';
const asyncDone = promisify(asyncDoneSync);
const exec = promisify(child_process.exec);
import xml2js from 'xml2js';
const parseXML = promisify(xml2js.parseString);
import { targetAndModuleCombinations, npmPkgName } from './util.js';
import { createRequire } from 'node:module';

const require = createRequire(import.meta.url);

const jestArgv = [];

if (argv.verbose) {
    jestArgv.push(`--verbose`);
} else {
    jestArgv.push(`--reporters=jest-silent-reporter`);
}

if (targetAndModuleCombinations.length > 1) {
    jestArgv.push(`--detectOpenHandles`);
}

const jest = path.join(path.parse(require.resolve(`jest`)).dir, `../bin/jest.js`);
const testOptions = {
    stdio: [`ignore`, `inherit`, `inherit`],
    env: {
        ...process.env,
        // hide fs.promises/stream[Symbol.asyncIterator] warnings
        NODE_NO_WARNINGS: `1`,
    },
};

export const testTask = ((cache, execArgv, testOptions) => memoizeTask(cache, function test(target, format) {
    const opts = { ...testOptions };
    const args = [...execArgv];
    if (format === 'esm' || target === 'ts' || target === 'src' || target === npmPkgName) {
        args.unshift(`--experimental-vm-modules`);
    }
    if (argv.coverage) {
        args.push(`-c`, `jestconfigs/jest.coverage.config.js`);
    } else {
        const cfgname = [target, format].filter(Boolean).join('.');
        args.push(`-c`, `jestconfigs/jest.${cfgname}.config.js`);
    }
    args.push(...(argv._unknown || []).filter((x) => x !== 'test'));
    args.push(...argv.tests);
    opts.env = {
        ...opts.env,
        TEST_TARGET: target,
        TEST_MODULE: format,
        TEST_DOM_STREAMS: (target === 'src' || format === 'umd').toString(),
        TEST_NODE_STREAMS: (target === 'src' || format !== 'umd').toString(),
        TEST_TS_SOURCE: !!argv.coverage || (target === 'src') || (opts.env.TEST_TS_SOURCE === 'true')
    };
    return asyncDone(() => child_process.spawn(`node`, args, opts));
}))({}, [jest, ...jestArgv], testOptions);
