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

import { RecordBatchStreamWriterOptions } from 'apache-arrow/ipc/writer.js';
import {
    generateDictionaryTables, generateRandomTables
} from '../../../data/tables.js';
import * as generate from '../../../generate-test-data.js';
import { validateRecordBatchIterator } from '../validate.js';

import {
    builderThroughIterable,
    compressionRegistry,
    CompressionType,
    Dictionary,
    Int32,
    RecordBatch,
    RecordBatchFileWriter,
    RecordBatchReader,
    Table,
    tableFromArrays,
    Uint32,
    Vector
} from 'apache-arrow';

import {
    extractCompressedPrefixes,
    FILE_FORMAT_HEADER_LENGTH,
    registerCompressionCodecs,
} from './compression-codecs.js';

describe('RecordBatchFileWriter', () => {
    for (const table of generateRandomTables([10, 20, 30])) {
        testFileWriter(table, `[${table.schema.fields.join(', ')}]`);
    }
    for (const table of generateDictionaryTables([10, 20, 30])) {
        testFileWriter(table, `${table.schema.fields[0]}`);
    }

    const compressionTypes = [CompressionType.LZ4_FRAME, CompressionType.ZSTD];
    beforeAll(async () => {
        await registerCompressionCodecs();
    });

    const table = generate.table([10, 20, 30]).table;
    for (const compressionType of compressionTypes) {
        const testName = `[${table.schema.fields.join(', ')}] - ${CompressionType[compressionType]} compressed`;
        testFileWriter(table, testName, { compressionType });
    }

    describe('compressed body buffer length prefix', () => {
        for (const compressionType of compressionTypes) {
            it(`writes the uncompressed length for ${CompressionType[compressionType]}`, async () => {
                // Highly compressible data so most buffers take the compressed branch.
                const fixture = tableFromArrays({
                    id: Int32Array.from({ length: 1000 }, (_, i) => i),
                    label: Array.from({ length: 1000 }, () => 'a highly compressible value'),
                });

                const bytes = await RecordBatchFileWriter.writeAll(fixture, { compressionType }).toUint8Array();
                const prefixes = extractCompressedPrefixes(
                    bytes.subarray(FILE_FORMAT_HEADER_LENGTH),
                    compressionRegistry.get(compressionType)!,
                );

                // Guard against a vacuous pass — the fixture must exercise the branch.
                expect(prefixes.length).toBeGreaterThan(0);
                for (const { prefix, decompressedLength } of prefixes) {
                    expect(prefix).toBe(decompressedLength);
                }
            });
        }
    });

    it('should throw if attempting to write replacement dictionary batches', async () => {
        const type = new Dictionary<Uint32, Int32>(new Uint32, new Int32, 0);
        const writer = new RecordBatchFileWriter();
        writer.write(new RecordBatch({
            // Clone the data with the original Dictionary type so the cloned chunk has id 0
            dictionary_encoded_uint32: generate.dictionary(50, 20, new Uint32, new Int32).vector.data[0].clone(type)
        }));
        expect(() => {
            writer.write(new RecordBatch({
                // Clone the data with the original Dictionary type so the cloned chunk has id 0
                dictionary_encoded_uint32: generate.dictionary(50, 20, new Uint32, new Int32).vector.data[0].clone(type)
            }));
        }).toThrow();
    });

    it('should write delta dictionary batches', async () => {

        const name = 'dictionary_encoded_uint32';
        const resultChunks: Vector<Dictionary<Uint32, Int32>>[] = [];
        const {
            vector: sourceVector, values: sourceValues,
        } = generate.dictionary(1000, 20, new Uint32(), new Int32());

        const writer = RecordBatchFileWriter.writeAll((function* () {
            const transform = builderThroughIterable({
                type: sourceVector.type, nullValues: [null],
                queueingStrategy: 'count', highWaterMark: 50,
            });
            for (const chunk of transform(sourceValues())) {
                resultChunks.push(chunk);
                yield new RecordBatch({ [name]: chunk.data[0] });
            }
        })());

        expect(new Vector(resultChunks)).toEqualVector(sourceVector);

        type T = { [name]: Dictionary<Uint32, Int32> };
        const sourceTable = new Table({ [name]: sourceVector });
        const resultTable = new Table(RecordBatchReader.from<T>(await writer.toUint8Array()));

        const child = resultTable.getChild(name)!;
        const dicts = child.data.map(({ dictionary }) => dictionary!);
        const dictionary = dicts[child.data.length - 1];

        expect(resultTable).toEqualTable(sourceTable);
        expect(dictionary).toBeInstanceOf(Vector);
        expect(dictionary.data).toHaveLength(20);
    });
});

function testFileWriter(table: Table, name: string, options?: RecordBatchStreamWriterOptions) {
    describe(`should write the Arrow IPC file format (${name})`, () => {
        test(`Table`, validateTable.bind(0, table, options));
    });
}

async function validateTable(source: Table, options?: RecordBatchStreamWriterOptions) {
    const writer = RecordBatchFileWriter.writeAll(source, options);
    const result = new Table(RecordBatchReader.from(await writer.toUint8Array()));
    validateRecordBatchIterator(3, source.batches);
    expect(result).toEqualTable(source);
}
