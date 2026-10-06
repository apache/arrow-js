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

import { Bool, DataType, Dictionary, Float32, Float64, Int32, Int8, Utf8, Schema, Field, makeTable, tableFromArrays, tableFromJSON, tableToIPC, tableFromIPC, Type } from 'apache-arrow';

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
});


describe('tableFromArrays() with schema', () => {
    test(`schema overrides number inference to Int32`, () => {
        const schema = new Schema([new Field('a', new Int32)]);
        const table = tableFromArrays({ a: [1, 2, 3] }, schema);
        expect(table.getChild('a')!.type).toBeInstanceOf(Int32);
        expect(table.numRows).toBe(3);
        expect(table.getChild('a')!.toArray()).toEqual(new Int32Array([1, 2, 3]));
    });

    test(`schema overrides string inference to Utf8`, () => {
        const schema = new Schema([new Field('b', new Utf8)]);
        const table = tableFromArrays({ b: ['a', 'b'] }, schema);
        expect(table.getChild('b')!.type).toBeInstanceOf(Utf8);
        expect(table.numRows).toBe(2);
        expect(table.getChild('b')!.get(0)).toBe('a');
        expect(table.getChild('b')!.get(1)).toBe('b');
    });

    test(`schema coerces TypedArray type`, () => {
        const schema = new Schema([new Field('a', new Int32)]);
        const table = tableFromArrays({ a: new Float32Array([1, 2, 3]) }, schema);
        expect(table.getChild('a')!.type).toBeInstanceOf(Int32);
        expect(table.getChild('a')!.toArray()).toEqual(new Int32Array([1, 2, 3]));
    });

    test(`preserves schema-level metadata`, () => {
        const schema = new Schema(
            [new Field('a', new Int32)],
            new Map([['source', 'test']])
        );
        const table = tableFromArrays({ a: [1, 2, 3] }, schema);
        expect(table.schema.metadata.get('source')).toBe('test');
    });

    test(`preserves field-level metadata`, () => {
        const schema = new Schema([
            new Field('a', new Int32, true, new Map([['unit', 'm']]))
        ]);
        const table = tableFromArrays({ a: [1, 2, 3] }, schema);
        expect(table.schema.fields[0].metadata.get('unit')).toBe('m');
    });

    test(`preserves field ordering from schema`, () => {
        const schema = new Schema([
            new Field('b', new Float64),
            new Field('a', new Int32),
        ]);
        const table = tableFromArrays({ a: [1, 2, 3], b: [4.0, 5.0, 6.0] }, schema);
        expect(table.schema.fields[0].name).toBe('b');
        expect(table.schema.fields[1].name).toBe('a');
        expect(table.getChild('b')!.type).toBeInstanceOf(Float64);
        expect(table.getChild('a')!.type).toBeInstanceOf(Int32);
    });

    test(`throws on missing schema field`, () => {
        const schema = new Schema([new Field('c', new Int32)]);
        expect(() => tableFromArrays({ a: [1] }, schema)).toThrow(TypeError);
        expect(() => tableFromArrays({ a: [1] }, schema)).toThrow(/Schema field "c" not found in input/);
    });

    test(`ignores extra input keys not in schema`, () => {
        const schema = new Schema([new Field('a', new Int32)]);
        const table = tableFromArrays({ a: [1, 2], b: [3, 4] }, schema);
        expect(table.numCols).toBe(1);
        expect(table.schema.fields[0].name).toBe('a');
    });

    test(`preserves nullability`, () => {
        const schema = new Schema([new Field('a', new Int32, false)]);
        const table = tableFromArrays({ a: [1, 2, 3] }, schema);
        expect(table.schema.fields[0].nullable).toBe(false);
    });

    test(`handles null values in input`, () => {
        const schema = new Schema([new Field('a', new Int32, true)]);
        const table = tableFromArrays({ a: [1, null, 3] }, schema);
        expect(table.numRows).toBe(3);
        expect(table.getChild('a')!.nullCount).toBeGreaterThan(0);
    });

    test(`BigInt boundary throws for BigInt64Array to Int32`, () => {
        const schema = new Schema([new Field('a', new Int32)]);
        expect(() => tableFromArrays({ a: new BigInt64Array([1n, 2n]) }, schema)).toThrow(TypeError);
        expect(() => tableFromArrays({ a: new BigInt64Array([1n, 2n]) }, schema)).toThrow(/BigInt/);
    });

    test(`handles empty arrays`, () => {
        const schema = new Schema([new Field('a', new Int32)]);
        const table = tableFromArrays({ a: new Int32Array(0) }, schema);
        expect(table.numRows).toBe(0);
        expect(table.numCols).toBe(1);
        expect(table.getChild('a')!.type).toBeInstanceOf(Int32);
    });
});

describe('tableFromArrays() with schema IPC round-trip', () => {
    const schema = new Schema([
        new Field('b', new Utf8),
        new Field('a', new Int32, true, new Map([['unit', 'meters']])),
        new Field('c', new Int8),
    ], new Map([['source', 'test']]));

    // Input key order differs from schema field order
    const input = {
        a: [1, null, 3],
        b: ['x', 'y', 'z'],
        c: new Float32Array([10, 20, 30]),
    };

    for (const format of ['stream', 'file'] as const) {
        test(`round-trips through IPC ${format} format`, () => {
            const original = tableFromArrays(input, schema);
            const buffer = tableToIPC(original, format);
            const table = tableFromIPC(buffer);

            // Row and column counts
            expect(table.numRows).toBe(3);
            expect(table.numCols).toBe(3);

            // Field ordering matches schema (not input key order)
            expect(table.schema.fields[0].name).toBe('b');
            expect(table.schema.fields[1].name).toBe('a');
            expect(table.schema.fields[2].name).toBe('c');

            // Types match schema-specified types (IPC reconstructs base classes, so check typeId + properties)
            const typeA = table.getChild('a')!.type;
            expect(DataType.isInt(typeA)).toBe(true);
            expect((typeA as any).bitWidth).toBe(32);

            const typeB = table.getChild('b')!.type;
            expect(typeB.typeId).toBe(Type.Utf8);

            const typeC = table.getChild('c')!.type;
            expect(DataType.isInt(typeC)).toBe(true);
            expect((typeC as any).bitWidth).toBe(8);

            // Schema-level metadata
            expect(table.schema.metadata.get('source')).toBe('test');

            // Field-level metadata
            const aField = table.schema.fields.find(f => f.name === 'a')!;
            expect(aField.metadata.get('unit')).toBe('meters');

            // Nullability and null counts
            expect(aField.nullable).toBe(true);
            expect(table.getChild('a')!.nullCount).toBe(1);

            // Data values
            const colA = table.getChild('a')!;
            expect(colA.get(0)).toBe(1);
            expect(colA.get(1)).toBeNull();
            expect(colA.get(2)).toBe(3);

            expect(table.getChild('b')!.get(0)).toBe('x');
            expect(table.getChild('b')!.get(1)).toBe('y');
            expect(table.getChild('b')!.get(2)).toBe('z');

            // TypedArray coercion: Float32Array input → Int8 output
            const colC = table.getChild('c')!;
            expect(colC.get(0)).toBe(10);
            expect(colC.get(1)).toBe(20);
            expect(colC.get(2)).toBe(30);
        });
    }
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
});
