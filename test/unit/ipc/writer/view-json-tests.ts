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
    BinaryView,
    Utf8View,
    RecordBatchJSONWriter,
    RecordBatchReader,
    Table,
    tableFromArrays,
    vectorFromArray
} from 'apache-arrow';

describe('BinaryView and Utf8View JSON serialization', () => {
    test('Utf8View with inline data (≤12 bytes) round-trips through JSON', async () => {
        // Create test data with strings that fit inline (≤12 bytes)
        const strings = ['Hello', 'World', 'Arrow', 'JS', '', 'Test123456'];
        const vector = vectorFromArray(strings, new Utf8View());
        const table = new Table({ data: vector });

        // Serialize to JSON
        const writer = RecordBatchJSONWriter.writeAll(table);
        const jsonString = await writer.toString();
        const json = JSON.parse(jsonString);

        // Deserialize from JSON
        const result = new Table(RecordBatchReader.from(json));

        // Verify round-trip
        expect(result.numRows).toBe(table.numRows);
        expect(result.getChild('data')?.toArray()).toEqual(strings);
    });

    test('Utf8View with out-of-line data (>12 bytes) round-trips through JSON', async () => {
        // Create test data with strings that require external buffers (>12 bytes)
        const strings = [
            'This is a longer string',
            'Another long string value',
            'Short',
            'Yet another string that exceeds 12 bytes',
            null
        ];
        const vector = vectorFromArray(strings, new Utf8View());
        const table = new Table({ data: vector });

        // Serialize to JSON
        const writer = RecordBatchJSONWriter.writeAll(table);
        const jsonString = await writer.toString();
        const json = JSON.parse(jsonString);

        // Verify JSON structure has VIEWS and VARIADIC_DATA_BUFFERS
        const batch = json.batches[0];
        const column = batch.columns[0];
        expect(column.VIEWS).toBeDefined();
        expect(column.VARIADIC_DATA_BUFFERS).toBeDefined();

        // Deserialize from JSON
        const result = new Table(RecordBatchReader.from(json));

        // Verify round-trip
        expect(result.numRows).toBe(table.numRows);
        expect(result.getChild('data')?.toArray()).toEqual(strings);
    });

    test('BinaryView with inline data round-trips through JSON', async () => {
        // Create test data with binary values that fit inline
        const binaries = [
            new Uint8Array([1, 2, 3, 4]),
            new Uint8Array([5, 6, 7]),
            new Uint8Array([]),
            new Uint8Array([0xFF, 0xAB, 0xCD, 0xEF, 0x12, 0x34])
        ];
        const vector = vectorFromArray(binaries, new BinaryView());
        const table = new Table({ data: vector });

        // Serialize to JSON
        const writer = RecordBatchJSONWriter.writeAll(table);
        const jsonString = await writer.toString();
        const json = JSON.parse(jsonString);

        // Verify JSON structure
        const batch = json.batches[0];
        const column = batch.columns[0];
        expect(column.VIEWS).toBeDefined();
        expect(Array.isArray(column.VIEWS)).toBe(true);

        // Deserialize from JSON
        const result = new Table(RecordBatchReader.from(json));

        // Verify round-trip
        expect(result.numRows).toBe(table.numRows);

        const resultArray = result.getChild('data')?.toArray() || [];
        for (const [i, binary] of binaries.entries()) {
            expect(resultArray[i]).toEqual(binary);
        }
    });

    test('BinaryView with out-of-line data round-trips through JSON', async () => {
        // Create test data with binary values that require external buffers (>12 bytes)
        const binaries = [
            new Uint8Array(Array.from({ length: 20 }, (_, i) => i)),
            new Uint8Array([1, 2, 3, 4, 5]),
            new Uint8Array(Array.from({ length: 50 }, (_, i) => i * 2)),
            null
        ];
        const vector = vectorFromArray(binaries, new BinaryView());
        const table = new Table({ data: vector });

        // Serialize to JSON
        const writer = RecordBatchJSONWriter.writeAll(table);
        const jsonString = await writer.toString();
        const json = JSON.parse(jsonString);

        // Verify JSON structure has VARIADIC_DATA_BUFFERS
        const batch = json.batches[0];
        const column = batch.columns[0];
        expect(column.VIEWS).toBeDefined();
        expect(column.VARIADIC_DATA_BUFFERS).toBeDefined();
        expect(column.VARIADIC_DATA_BUFFERS.length).toBeGreaterThan(0);

        // Deserialize from JSON
        const result = new Table(RecordBatchReader.from(json));

        // Verify round-trip
        expect(result.numRows).toBe(table.numRows);

        const resultArray = result.getChild('data')?.toArray() || [];
        for (const [i, binary] of binaries.entries()) {
            if (binary === null) {
                expect(resultArray[i]).toBeNull();
            } else {
                expect(resultArray[i]).toEqual(binary);
            }
        }
    });

    test('Utf8View JSON distinguishes between inline hex (BinaryView) and UTF-8 strings', async () => {
        // This test verifies the bug fix: Utf8View INLINED should be UTF-8 strings, not hex
        const strings = ['Hello', 'World'];
        const vector = vectorFromArray(strings, new Utf8View());
        const table = new Table({ data: vector });

        // Serialize to JSON
        const writer = RecordBatchJSONWriter.writeAll(table);
        const jsonString = await writer.toString();
        const json = JSON.parse(jsonString);

        // Check that INLINED values are UTF-8 strings, not hex
        const views = json.batches[0].columns[0].VIEWS;
        expect(views[0].INLINED).toBe('Hello');
        expect(views[1].INLINED).toBe('World');

        // NOT hex strings like "48656C6C6F"
        expect(views[0].INLINED).not.toMatch(/^[0-9A-F]+$/);
    });
});
