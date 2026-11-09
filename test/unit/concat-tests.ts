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

import { tableFromArrays, vectorFromArray } from 'apache-arrow';

/**
 * Defining `Symbol.isConcatSpreadable` on any object, whatever its value,
 * sends every `Array.prototype.concat` in the V8 process down its slow path.
 *
 * @see https://github.com/apache/arrow-js/issues/525
 */
describe('Array.prototype.concat with Arrow objects', () => {
    const table = tableFromArrays({ x: Int32Array.from([1, 2, 3]) });
    const cases = [
        ['Vector', vectorFromArray([1, 2, 3])],
        ['Vector<Utf8>', vectorFromArray(['a', 'b', 'c'])],
        ['Table', table],
        ['RecordBatch', table.batches[0]],
    ] as const;

    test.each(cases)('%s does not define Symbol.isConcatSpreadable', (_, value) => {
        expect(Symbol.isConcatSpreadable in value).toBe(false);
    });

    test.each(cases)('[0].concat(%s) appends the object itself', (_, value) => {
        const result = ([0] as unknown[]).concat(value);
        expect(result).toHaveLength(2);
        expect(result[0]).toBe(0);
        expect(result[1]).toBe(value);
    });
});
