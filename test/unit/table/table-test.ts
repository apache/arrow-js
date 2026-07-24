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

import { Bool, Dictionary, Float32, Float64, Int32, Int8, makeTable, tableFromArrays, tableFromJSON } from 'apache-arrow';

describe('makeTable()', () => {
    test(`creates a new Table from Typed Arrays`, () => {
        const i32s = Int32Array.from({ length: 10 }, (_, i) => i);
        const f32s = Float32Array.from({ length: 10 }, (_, i) => i);
        const table = makeTable({ i32s, f32s });
        const i32 = table.getChild('i32s')!;
        const f32 = table.getChild('f32s')!;
        expect(table.getChild('foo' as any)).toBeNull();

        expect(table.numRows).toBe(10);
        expect(i32.type).toBeInstanceOf(Int32);
        expect(f32.type).toBeInstanceOf(Float32);
        expect(i32).toHaveLength(10);
        expect(f32).toHaveLength(10);
        expect(i32.toArray()).toBeInstanceOf(Int32Array);
        expect(f32.toArray()).toBeInstanceOf(Float32Array);
        expect(i32.toArray()).toEqual(i32s);
        expect(f32.toArray()).toEqual(f32s);
    });
});

describe('tableFromArrays()', () => {
    test(`creates table from typed arrays and JavaScript arrays`, () => {
        const table = tableFromArrays({
            a: new Float32Array([1, 2]),
            b: new Int8Array([1, 2]),
            c: [1, 2, 3],
            d: ['foo', 'bar'],
        });

        expect(table.numRows).toBe(3);
        expect(table.numCols).toBe(4);

        expect(table.getChild('a')!.type).toBeInstanceOf(Float32);
        expect(table.getChild('b')!.type).toBeInstanceOf(Int8);
        expect(table.getChild('c')!.type).toBeInstanceOf(Float64);
        expect(table.getChild('d')!.type).toBeInstanceOf(Dictionary);
        expect(table.getChild('e' as any)).toBeNull();
    });

    test(`creates table from arrays of strings`, () => {
        const table = tableFromArrays({ word: [['a', 'b'], ['c', 'd']] });
        expect(table.numRows).toBe(2);
        const word = table.getChild('word')!;
        expect(word.get(0)!.toArray()).toEqual(['a', 'b']);
        expect(word.get(1)!.toArray()).toEqual(['c', 'd']);
    });

    test(`creates table from arrays of objects containing strings`, () => {
        const table = tableFromArrays({ customers: [{ names: ['joe'] }, { names: ['bob'] }] });
        expect(table.numRows).toBe(2);
        const customers = table.getChild('customers')!;
        expect(customers.get(0)!.names.toArray()).toEqual(['joe']);
        expect(customers.get(1)!.names.toArray()).toEqual(['bob']);
    });
});


describe('tableFromJSON()', () => {
    test(`creates table from array of objects`, () => {
        const table = tableFromJSON([{
            a: 42,
            b: true,
            c: 'foo',
        }, {
            a: 12,
            b: false,
            c: 'bar',
        }]);

        expect(table.numRows).toBe(2);
        expect(table.numCols).toBe(3);

        expect(table.getChild('a')!.type).toBeInstanceOf(Float64);
        expect(table.getChild('b')!.type).toBeInstanceOf(Bool);
        expect(table.getChild('c')!.type).toBeInstanceOf(Dictionary);
    });

    test(`handles arrays of strings`, () => {
        const t1 = tableFromJSON([{ a: ['hi'] }]);
        expect(t1.getChild('a')!.get(0)!.toArray()).toEqual(['hi']);

        const t2 = tableFromJSON([{ a: ['hi', 'there'] }]);
        expect(t2.getChild('a')!.get(0)!.toArray()).toEqual(['hi', 'there']);
    });

    test(`handles nested objects containing strings`, () => {
        const table = tableFromJSON([{ a: [{ b: 'hi' }, { b: 'there' }] }]);
        expect(table.numRows).toBe(1);
        const rows = table.getChild('a')!.get(0)!;
        expect(rows.get(0)!.b).toBe('hi');
        expect(rows.get(1)!.b).toBe('there');
    });
});
