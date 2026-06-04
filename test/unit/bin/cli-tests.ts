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

import { parseCliArgs, formatUsage, OptionSpec } from '../../../src/bin/cli.js';

describe(`parseCliArgs`, () => {
    const spec: OptionSpec[] = [
        { name: 'schema', alias: 's', type: String, multiple: true },
        { name: 'file', alias: 'f', type: String, multiple: true },
        { name: 'sep', type: String, defaultValue: ' | ' },
        { name: 'count', type: Number },
        { name: 'metadata', alias: 'm', type: Boolean },
        { name: 'help', type: Boolean },
    ];

    test('parses a boolean long flag', () => {
        const r = parseCliArgs(spec, ['--help']);
        expect(r.values.help).toBe(true);
        expect(r.positionals).toEqual([]);
    });

    test('parses a short alias', () => {
        const r = parseCliArgs(spec, ['-m']);
        expect(r.values.metadata).toBe(true);
    });

    test('parses repeated multi-flag', () => {
        const r = parseCliArgs(spec, ['-s', 'a', '-s', 'b', '-s', 'c']);
        expect(r.values.schema).toEqual(['a', 'b', 'c']);
        expect(r.positionals).toEqual([]);
    });

    test('parses space-delimited multi-flag (greedy)', () => {
        const r = parseCliArgs(spec, ['-s', 'a', 'b', 'c']);
        expect(r.values.schema).toEqual(['a', 'b', 'c']);
        expect(r.positionals).toEqual([]);
    });

    test('greedy attribution stops at the next flag', () => {
        const r = parseCliArgs(spec, ['-s', 'a', 'b', 'c', '-f', 'x.arrow']);
        expect(r.values.schema).toEqual(['a', 'b', 'c']);
        expect(r.values.file).toEqual(['x.arrow']);
        expect(r.positionals).toEqual([]);
    });

    test('greedy attribution spans across multi-flags', () => {
        const r = parseCliArgs(spec, ['-s', 'a', 'b', 'c', '-f', 'x.arrow', 'pos.arrow']);
        expect(r.values.schema).toEqual(['a', 'b', 'c']);
        expect(r.values.file).toEqual(['x.arrow', 'pos.arrow']);
        expect(r.positionals).toEqual([]);
    });

    test('positionals with no preceding multi-flag stay in positionals', () => {
        const r = parseCliArgs(spec, ['file1.arrow', 'file2.arrow']);
        expect(r.values.schema).toBeUndefined();
        expect(r.positionals).toEqual(['file1.arrow', 'file2.arrow']);
    });

    test('default values are populated when flag is absent', () => {
        const r = parseCliArgs(spec, []);
        expect(r.values.sep).toBe(' | ');
    });

    test('user-supplied value overrides default', () => {
        const r = parseCliArgs(spec, ['--sep', ' , ']);
        expect(r.values.sep).toBe(' , ');
    });

    test('unknown flags do not throw, and break greedy attribution', () => {
        const r = parseCliArgs(spec, ['-s', 'a', '--unknown', 'b', '-f', 'x.arrow']);
        expect(r.values.schema).toEqual(['a']);
        expect(r.values.unknown).toBe(true);
        expect(r.positionals).toEqual(['b']);
        expect(r.values.file).toEqual(['x.arrow']);
    });

    test('Number type is coerced from string', () => {
        const r = parseCliArgs(spec, ['--count', '42']);
        expect(r.values.count).toBe(42);
        expect(typeof r.values.count).toBe('number');
    });

    test('combined: typical arrow2csv invocation', () => {
        const r = parseCliArgs(spec, ['-s', 'foo', 'bar', '-f', 'simple.arrow', '-m']);
        expect(r.values.schema).toEqual(['foo', 'bar']);
        expect(r.values.file).toEqual(['simple.arrow']);
        expect(r.values.metadata).toBe(true);
    });
});

describe(`formatUsage`, () => {
    test('renders header and content sections', () => {
        const out = formatUsage([
            { header: 'arrow2csv', content: 'Print a CSV from an Arrow file' },
        ]);
        expect(out).toContain('arrow2csv');
        expect(out).toContain('Print a CSV from an Arrow file');
    });

    test('strips {bold ...} and {underline ...} markup', () => {
        const out = formatUsage([
            { header: 'Synopsis', content: ['$ arrow2csv {bold -s} col1 {underline file.arrow}'] },
        ]);
        expect(out).toContain('$ arrow2csv -s col1 file.arrow');
        expect(out).not.toMatch(/\{bold/);
        expect(out).not.toMatch(/\{underline/);
    });

    test('renders an option list with aliases and descriptions', () => {
        const out = formatUsage([
            {
                header: 'Options',
                optionList: [
                    { name: 'schema', alias: 's', type: String, multiple: true, description: 'Column names' },
                    { name: 'help', type: Boolean, description: 'Print this usage guide.' },
                ],
            },
        ]);
        expect(out).toContain('-s, --schema');
        expect(out).toContain('Column names');
        expect(out).toContain('--help');
        expect(out).toContain('Print this usage guide.');
    });

    test('option list renders correctly when no options have aliases', () => {
        const out = formatUsage([
            {
                header: 'Options',
                optionList: [
                    { name: 'mode', type: String, description: 'The mode' },
                ],
            },
        ]);
        expect(out).toContain('--mode');
        expect(out).toContain('The mode');
    });

    test('renders type labels and synthesised type hints', () => {
        const out = formatUsage([
            {
                header: 'Options',
                optionList: [
                    { name: 'schema', alias: 's', type: String, multiple: true, typeLabel: '{underline columns}', description: 'Column names' },
                    { name: 'file', alias: 'f', type: String, multiple: true, description: 'The Arrow file to read' },
                    { name: 'sep', type: String, description: 'Column separator' },
                    { name: 'metadata', alias: 'm', type: Boolean, description: 'Print metadata' },
                ],
            },
        ]);
        expect(out).toContain('-s, --schema columns');
        expect(out).toContain('-f, --file string[]');
        expect(out).toContain('--sep string');
        // Boolean options get no type label
        expect(out).toMatch(/-m, --metadata\s+Print metadata/);
        expect(out).not.toMatch(/--metadata\s+(string|boolean)/);
    });
});
