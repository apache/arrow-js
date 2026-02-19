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

import {
    Field,
    Int32,
    makeData,
    RecordBatch,
    RecordBatchFileWriter,
    RecordBatchStreamWriter,
    Schema,
    Struct,
    tableFromIPC,
    Utf8
} from 'apache-arrow';

describe('RecordBatch message metadata writing', () => {

    // Helper to create a simple RecordBatch for testing
    function createTestBatch(): RecordBatch {
        const schema = new Schema([
            new Field('id', new Int32()),
            new Field('name', new Utf8())
        ]);
        const idData = makeData({ type: new Int32(), data: new Int32Array([1, 2, 3]) });
        const nameData = makeData({ type: new Utf8(), data: Buffer.from('foobarbaz'), valueOffsets: new Int32Array([0, 3, 6, 9]) });
        const structData = makeData({
            type: new Struct(schema.fields),
            length: 3,
            nullCount: 0,
            children: [idData, nameData]
        });
        return new RecordBatch(schema, structData);
    }

    describe('Stream format round-trip', () => {
        test('should write and read metadata via RecordBatchStreamWriter', () => {
            const batch = createTestBatch();
            batch.metadata.set('batch_id', '123');
            batch.metadata.set('source', 'test');

            const writer = new RecordBatchStreamWriter();
            writer.write(batch);
            writer.finish();
            const buffer = writer.toUint8Array(true);

            const table = tableFromIPC(buffer);
            expect(table.batches).toHaveLength(1);
            expect(table.batches[0].metadata).toBeInstanceOf(Map);
            expect(table.batches[0].metadata.get('batch_id')).toBe('123');
            expect(table.batches[0].metadata.get('source')).toBe('test');
        });

        test('should write batch without metadata when none provided', () => {
            const batch = createTestBatch();

            const writer = new RecordBatchStreamWriter();
            writer.write(batch);
            writer.finish();
            const buffer = writer.toUint8Array(true);

            const table = tableFromIPC(buffer);
            expect(table.batches).toHaveLength(1);
            expect(table.batches[0].metadata).toBeInstanceOf(Map);
            expect(table.batches[0].metadata.size).toBe(0);
        });
    });

    describe('File format round-trip', () => {
        test('should write and read metadata via RecordBatchFileWriter', () => {
            const batch = createTestBatch();
            batch.metadata.set('format', 'file');
            batch.metadata.set('version', '1.0');

            const writer = new RecordBatchFileWriter();
            writer.write(batch);
            writer.finish();
            const buffer = writer.toUint8Array(true);

            const table = tableFromIPC(buffer);
            expect(table.batches).toHaveLength(1);
            expect(table.batches[0].metadata.get('format')).toBe('file');
            expect(table.batches[0].metadata.get('version')).toBe('1.0');
        });
    });

    describe('Multiple batches with different metadata', () => {
        test('should write multiple batches with different metadata', () => {
            const writer = new RecordBatchStreamWriter();

            const batch1 = createTestBatch();
            batch1.metadata.set('batch_index', '0');
            batch1.metadata.set('tag', 'first');

            const batch2 = createTestBatch();
            batch2.metadata.set('batch_index', '1');
            batch2.metadata.set('tag', 'middle');

            const batch3 = createTestBatch();
            batch3.metadata.set('batch_index', '2');
            batch3.metadata.set('tag', 'last');

            writer.write(batch1);
            writer.write(batch2);
            writer.write(batch3);
            writer.finish();

            const table = tableFromIPC(writer.toUint8Array(true));
            expect(table.batches).toHaveLength(3);
            expect(table.batches[0].metadata.get('batch_index')).toBe('0');
            expect(table.batches[0].metadata.get('tag')).toBe('first');
            expect(table.batches[1].metadata.get('batch_index')).toBe('1');
            expect(table.batches[1].metadata.get('tag')).toBe('middle');
            expect(table.batches[2].metadata.get('batch_index')).toBe('2');
            expect(table.batches[2].metadata.get('tag')).toBe('last');
        });
    });

    describe('Metadata preservation through operations', () => {
        test('should preserve metadata through slice after round-trip', () => {
            const batch = createTestBatch();
            batch.metadata.set('key', 'value');

            const writer = new RecordBatchStreamWriter();
            writer.write(batch);
            writer.finish();

            const table = tableFromIPC(writer.toUint8Array(true));
            const sliced = table.batches[0].slice(0, 2);

            expect(sliced.metadata.get('key')).toBe('value');
        });

        test('should preserve metadata through selectAt after round-trip', () => {
            const batch = createTestBatch();
            batch.metadata.set('preserved', 'yes');

            const writer = new RecordBatchStreamWriter();
            writer.write(batch);
            writer.finish();

            const table = tableFromIPC(writer.toUint8Array(true));
            const selected = table.batches[0].selectAt([0]);

            expect(selected.metadata.get('preserved')).toBe('yes');
        });
    });

    describe('Metadata from constructor', () => {
        test('should use metadata passed to RecordBatch constructor', () => {
            const schema = new Schema([new Field('id', new Int32())]);
            const idData = makeData({ type: new Int32(), data: new Int32Array([1, 2, 3]) });
            const structData = makeData({
                type: new Struct(schema.fields),
                length: 3,
                nullCount: 0,
                children: [idData]
            });
            const batch = new RecordBatch(schema, structData, new Map([['from_batch', 'value']]));

            const writer = new RecordBatchStreamWriter();
            writer.write(batch);
            writer.finish();

            const table = tableFromIPC(writer.toUint8Array(true));
            expect(table.batches[0].metadata.get('from_batch')).toBe('value');
        });
    });

    describe('Edge cases', () => {
        test('should handle empty metadata map', () => {
            const batch = createTestBatch();

            const writer = new RecordBatchStreamWriter();
            writer.write(batch);
            writer.finish();

            const table = tableFromIPC(writer.toUint8Array(true));
            expect(table.batches[0].metadata.size).toBe(0);
        });

        test('should handle unicode keys and values', () => {
            const batch = createTestBatch();
            batch.metadata.set('日本語キー', 'Japanese key');
            batch.metadata.set('emoji', '🎉🚀💻');
            batch.metadata.set('mixed', 'Hello 世界 مرحبا');

            const writer = new RecordBatchStreamWriter();
            writer.write(batch);
            writer.finish();

            const table = tableFromIPC(writer.toUint8Array(true));
            expect(table.batches[0].metadata.get('日本語キー')).toBe('Japanese key');
            expect(table.batches[0].metadata.get('emoji')).toBe('🎉🚀💻');
            expect(table.batches[0].metadata.get('mixed')).toBe('Hello 世界 مرحبا');
        });

        test('should handle empty string values', () => {
            const batch = createTestBatch();
            batch.metadata.set('empty_value', '');
            batch.metadata.set('normal', 'value');

            const writer = new RecordBatchStreamWriter();
            writer.write(batch);
            writer.finish();

            const table = tableFromIPC(writer.toUint8Array(true));
            expect(table.batches[0].metadata.get('empty_value')).toBe('');
            expect(table.batches[0].metadata.get('normal')).toBe('value');
        });

        test('should handle JSON string as metadata value', () => {
            const batch = createTestBatch();
            const jsonValue = JSON.stringify({ nested: { data: [1, 2, 3] }, flag: true });
            batch.metadata.set('json_data', jsonValue);

            const writer = new RecordBatchStreamWriter();
            writer.write(batch);
            writer.finish();

            const table = tableFromIPC(writer.toUint8Array(true));
            const retrieved = table.batches[0].metadata.get('json_data')!;
            const parsed = JSON.parse(retrieved);
            expect(parsed.nested.data).toEqual([1, 2, 3]);
            expect(parsed.flag).toBe(true);
        });

        test('should handle long metadata values', () => {
            const batch = createTestBatch();
            const longValue = 'x'.repeat(10000);
            batch.metadata.set('long_value', longValue);

            const writer = new RecordBatchStreamWriter();
            writer.write(batch);
            writer.finish();

            const table = tableFromIPC(writer.toUint8Array(true));
            expect(table.batches[0].metadata.get('long_value')).toBe(longValue);
        });
    });
});
