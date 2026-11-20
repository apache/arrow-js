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

import { generateRandomTables } from '../../data/tables.js';
import { ArrowIOTestHelper } from './helpers.js';

import { AsyncMessageReader, MessageReader, tableFromArrays, tableFromIPC, tableToIPC } from 'apache-arrow';

for (const table of generateRandomTables([10, 20, 30])) {

    const io = ArrowIOTestHelper.stream(table);
    const name = `[\n ${table.schema.fields.join(',\n ')}\n]`;

    const numDictionaries = table.batches.reduce((dictionaries, batch) => {
        return [...batch.dictionaries.values()]
            .flatMap((dictionary) => dictionary.data)
            .reduce((dictionaries, data) => dictionaries.add(data), dictionaries);
    }, new Set()).size;

    const numMessages = /* schema message */ 1 +
                        /* recordBatch messages */ table.batches.length +
                        /* dictionary messages */ numDictionaries;

    const validate = validateMessageReader.bind(0, numMessages);
    const validateAsync = validateAsyncMessageReader.bind(0, numMessages);

    describe(`MessageReader (${name})`, () => {
        describe(`should read all Messages`, () => {
            test(`Uint8Array`, io.buffer(validate));
            test(`Iterable`, io.iterable(validate));
        });
    });

    describe(`AsyncMessageReader (${name})`, () => {
        describe(`should read all Messages`, () => {
            test('AsyncIterable', io.asyncIterable(validateAsync));
            test('fs.FileHandle', io.fsFileHandle(validateAsync));
            test('fs.ReadStream', io.fsReadableStream(validateAsync));
            test('stream.Readable', io.nodeReadableStream(validateAsync));
            test('whatwg.ReadableStream', io.whatwgReadableStream(validateAsync));
            test('whatwg.ReadableByteStream', io.whatwgReadableByteStream(validateAsync));
        });
    });
}

export function validateMessageReader(numMessages: number, source: any) {
    const reader = new MessageReader(source);
    let index = 0;
    for (const message of reader) {

        if (index === 0) {
            expect(message.isSchema()).toBe(true);
            expect(message.bodyLength).toBe(0);
        } else {
            expect(message.isSchema()).toBe(false);
            expect(message.isRecordBatch() || message.isDictionaryBatch()).toBe(true);
        }

        try {
            expect(message.bodyLength % 8).toBe(0);
        } catch (e) { throw new Error(`bodyLength: ${e}`); }

        const body = reader.readMessageBody(message.bodyLength);
        expect(body).toBeInstanceOf(Uint8Array);
        expect(body.byteLength).toBe(message.bodyLength);
        expect(index++).toBeLessThan(numMessages);
    }
    expect(index).toBe(numMessages);
    reader.return();
}

export async function validateAsyncMessageReader(numMessages: number, source: any) {
    const reader = new AsyncMessageReader(source);
    let index = 0;
    for await (const message of reader) {

        if (index === 0) {
            expect(message.isSchema()).toBe(true);
            expect(message.bodyLength).toBe(0);
        } else {
            expect(message.isSchema()).toBe(false);
            expect(message.isRecordBatch() || message.isDictionaryBatch()).toBe(true);
        }

        try {
            expect(message.bodyLength % 8).toBe(0);
        } catch (e) { throw new Error(`bodyLength: ${e}`); }

        const body = await reader.readMessageBody(message.bodyLength);
        expect(body).toBeInstanceOf(Uint8Array);
        expect(body.byteLength).toBe(message.bodyLength);
        expect(index++).toBeLessThan(numMessages);
    }
    expect(index).toBe(numMessages);
    await reader.return();
}

describe(`a stream that ends inside a message`, () => {
    const stream = tableToIPC(tableFromArrays({ x: Int32Array.of(1, 2, 3) }), 'stream');
    // The Schema message has no body, so the RecordBatch message starts after its metadata.
    const view = new DataView(stream.buffer, stream.byteOffset, stream.byteLength);
    const recordBatchStart = 8 + view.getInt32(4, true);

    // 2 and 6: inside the continuation marker and the metadata length.
    // 4 and 8: after them, with the metadata length or the metadata missing.
    for (const cut of [2, 4, 6, 8]) {
        const bytes = stream.subarray(0, recordBatchStart + cut);

        test(`MessageReader throws ${cut} bytes into the message prefix`, () => {
            expect(() => Array.from(new MessageReader(bytes))).toThrow(/but only read/);
        });

        test(`AsyncMessageReader throws ${cut} bytes into the message prefix`, async () => {
            const source = (async function* () { yield bytes; })();
            const readAll = async () => {
                const messages = [];
                for await (const message of new AsyncMessageReader(source)) {
                    messages.push(message);
                }
                return messages;
            };
            await expect(readAll()).rejects.toThrow(/but only read/);
        });

        test(`tableFromIPC throws ${cut} bytes into the message prefix`, () => {
            expect(() => tableFromIPC(bytes)).toThrow(/but only read/);
        });
    }

    test(`a stream without the end-of-stream marker is still read`, () => {
        const table = tableFromIPC(stream.subarray(0, stream.byteLength - 8));
        expect(table.numRows).toBe(3);
    });
});
