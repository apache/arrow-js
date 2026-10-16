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

import { jest } from '@jest/globals';
import * as Arrow from 'apache-arrow';

/**
 * arrow-js has had bugs where it behaves strangely or incorrectly when
 * more than one copy of the library is loaded, such as a duplicate install
 * or the CommonJS and ES module builds loaded together. This suite
 * validates that objects created by one copy can be used by another.
 *
 * The second copy is the same entry point evaluated again in a fresh
 * module registry, which gives it its own set of classes.
 *
 * @see https://github.com/apache/arrow-js/issues/61
 * @see https://github.com/apache/arrow-js/issues/496
 */
describe('Multiple copies of arrow-js', () => {

    let Other: typeof Arrow;

    beforeAll(async () => {
        jest.resetModules();
        Other = await import('apache-arrow');
    });

    test('loads a second copy of the library', () => {
        expect(Other.Table).not.toBe(Arrow.Table);
        expect(Other.DataType).not.toBe(Arrow.DataType);
    });

    const tables: Record<string, () => Arrow.Table> = {
        Int32: () => new Arrow.Table({ a: Arrow.vectorFromArray([1, null, 3], new Arrow.Int32) }),
        Float64: () => new Arrow.Table({ a: Arrow.vectorFromArray([1.5, null, 3.5], new Arrow.Float64) }),
        Bool: () => new Arrow.Table({ a: Arrow.vectorFromArray([true, null, false], new Arrow.Bool) }),
        Utf8: () => new Arrow.Table({ a: Arrow.vectorFromArray(['a', null, 'c'], new Arrow.Utf8) }),
        Timestamp: () => new Arrow.Table({ a: Arrow.vectorFromArray([new Date(0), null], new Arrow.TimestampMillisecond) }),
        Dictionary: () => new Arrow.Table({
            a: Arrow.vectorFromArray(['a', null, 'a'], new Arrow.Dictionary(new Arrow.Utf8, new Arrow.Int16))
        }),
        List: () => new Arrow.Table({
            a: Arrow.vectorFromArray([[1, 2], null, [3]], new Arrow.List(new Arrow.Field('item', new Arrow.Int32)))
        }),
        Struct: () => new Arrow.Table({
            a: Arrow.vectorFromArray([{ x: 1, y: 'a' }, null], new Arrow.Struct([
                new Arrow.Field('x', new Arrow.Int32),
                new Arrow.Field('y', new Arrow.Utf8),
            ]))
        }),
        'multiple columns': () => Arrow.tableFromArrays({ id: Int32Array.from([1, 2]), text: ['a', 'b'] }),
    };

    describe.each(['stream', 'file'] as const)('tableToIPC(%s)', (format) => {
        test.each(Object.keys(tables))('writes a %s table from another copy of the library', (name) => {
            const table = tables[name]();
            const expected = Arrow.tableToIPC(table, format);
            const actual = Other.tableToIPC(table as any, format);
            expect(actual).toEqual(expected);
            expect(Arrow.tableFromIPC(actual).toString()).toEqual(table.toString());
        });
    });

    test('visitors dispatch on a DataType from another copy of the library', () => {
        const vector = Other.vectorFromArray([1, 2, 3], new Arrow.Int32 as any);
        expect(vector.type.typeId).toBe(Arrow.Type.Int);
        expect([...vector]).toEqual([1, 2, 3]);
    });
});
