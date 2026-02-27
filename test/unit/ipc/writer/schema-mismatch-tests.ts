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

import '../../../jest-extensions.js';
import {
    Field,
    Float64,
    Int32,
    makeData,
    RecordBatch,
    RecordBatchStreamWriter,
    Schema,
    Struct,
    Utf8,
} from 'apache-arrow';

function createBatch(schema: Schema, data: Record<string, any>, length: number): RecordBatch {
    const children = schema.fields.map(field => {
        const d = data[field.name];
        if (field.type instanceof Utf8) {
            return makeData({
                type: field.type,
                data: Buffer.from(d.values),
                valueOffsets: new Int32Array(d.offsets),
            });
        }
        return makeData({ type: field.type, data: d });
    });
    const structData = makeData({
        type: new Struct(schema.fields),
        length,
        nullCount: 0,
        children,
    });
    return new RecordBatch(schema, structData);
}

describe('RecordBatchStreamWriter schema mismatch behavior', () => {

    const schemaA = new Schema([
        new Field('id', new Int32()),
        new Field('name', new Utf8()),
    ]);

    const schemaB = new Schema([
        new Field('x', new Float64()),
        new Field('y', new Float64()),
    ]);

    function makeBatchA(): RecordBatch {
        return createBatch(schemaA, {
            id: new Int32Array([1, 2, 3]),
            name: { values: 'foobarbaz', offsets: [0, 3, 6, 9] },
        }, 3);
    }

    function makeBatchB(): RecordBatch {
        return createBatch(schemaB, {
            x: new Float64Array([1.1, 2.2]),
            y: new Float64Array([3.3, 4.4]),
        }, 2);
    }

    test('default (autoDestroy=true): writing a mismatched batch throws an error', () => {
        const writer = new RecordBatchStreamWriter(); // autoDestroy defaults to true
        const batchA = makeBatchA();
        const batchB = makeBatchB();

        // Write the first batch — this establishes the schema
        writer.write(batchA);

        // Write a batch with a different schema — this should throw
        expect(() => writer.write(batchB)).toThrow(
            'RecordBatch schema does not match the writer\'s schema.'
        );
    });

    test('autoDestroy=false: writing a mismatched batch resets the schema and writes both batches as separate streams', async () => {
        const writer = new RecordBatchStreamWriter({ autoDestroy: false });
        const batchA = makeBatchA();
        const batchB = makeBatchB();

        // Write first batch
        writer.write(batchA);

        // Write batch with different schema — with autoDestroy=false,
        // this resets the writer to the new schema and writes the batch
        writer.write(batchB);
        writer.close();

        // The output contains two separate IPC streams.
        // RecordBatchReader.readAll should be able to read both.
        const { RecordBatchReader } = await import('apache-arrow');
        const buffer = await writer.toUint8Array();
        const readers = [];
        for await (const reader of RecordBatchReader.readAll(buffer)) {
            readers.push(new (await import('apache-arrow')).Table(await reader.readAll()));
        }

        expect(readers).toHaveLength(2);
        expect(readers[0].numRows).toBe(3);
        expect(readers[0].schema.fields.map(f => f.name)).toEqual(['id', 'name']);
        expect(readers[1].numRows).toBe(2);
        expect(readers[1].schema.fields.map(f => f.name)).toEqual(['x', 'y']);
    });

    test('writing to a closed writer throws an error', () => {
        const writer = new RecordBatchStreamWriter();
        const batchA = makeBatchA();

        writer.write(batchA);
        writer.close();

        // Now try to write again — the writer is closed
        expect(() => writer.write(makeBatchA())).toThrow();
    });

    test('schema mismatch error includes both schemas', () => {
        const writer = new RecordBatchStreamWriter();
        const batchA = makeBatchA();
        const batchB = makeBatchB();

        writer.write(batchA);

        expect(() => writer.write(batchB)).toThrow(/Expected:.*\n.*Received:/);
    });
});
