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

import { createIsValidFunction } from 'apache-arrow/builder/valid';

describe('Null sentinel validity predicates', () => {
    test('empty sentinel lists accept every value, including NaN', () => {
        for (const nullValues of [undefined, []]) {
            const isValid = createIsValidFunction(nullValues);
            expect([null, undefined, Number.NaN, 0, ''].every(value => isValid(value))).toBe(true);
        }
    });

    test.each([null, undefined, Number.NaN, 0, false, '', 'quoted "text"\nwith\\slashes', 42n])(
        'matches a single sentinel without coercion: %p', nullValue => {
            const isValid = createIsValidFunction([nullValue]);
            expect(isValid(nullValue)).toBe(false);
            expect(isValid(Symbol('different'))).toBe(true);
        }
    );

    test('multiple sentinels preserve NaN, zero and primitive type distinctions', () => {
        const isValid = createIsValidFunction([null, undefined, Number.NaN, -0, 42n, '42', false]);
        expect([null, undefined, Number.NaN, 0, -0, 42n, '42', false].map(value => isValid(value))).toEqual(new Array(8).fill(false));
        expect([42, true, '', 'NaN', 1n].every(value => isValid(value))).toBe(true);
    });

    test('snapshots sentinel lists, including duplicate entries', () => {
        const nullValues = ['missing', 'missing'];
        const isValid = createIsValidFunction(nullValues);
        nullValues.splice(0, 2, 'changed');
        expect(isValid('missing')).toBe(false);
        expect(isValid('changed')).toBe(true);
    });

    test('does not require the Function constructor permitted by unsafe-eval', () => {
        const originalFunction = globalThis.Function;
        let results: boolean[];
        try {
            globalThis.Function = (() => { throw new Error('Dynamic compilation blocked'); }) as unknown as FunctionConstructor;
            const isValid = createIsValidFunction([null, 'quoted "text"\nwith\\slashes', Number.NaN]);
            results = [null, Number.NaN, 'valid'].map(value => isValid(value));
        } finally {
            globalThis.Function = originalFunction;
        }
        expect(results!).toEqual([false, false, true]);
    });
});
