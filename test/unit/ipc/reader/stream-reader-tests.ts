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

import { generateRandomTables } from '../../../data/tables.js';
import { ArrowIOTestHelper } from '../helpers.js';
import {
    validateAsyncRecordBatchReader,
    validateRecordBatchReader
} from '../validate.js';

import {
    AsyncByteStream,
    ByteStream,
    Message,
    RecordBatchReader,
    RecordBatchStreamWriter,
    Table,
    tableFromArrays
} from 'apache-arrow';

for (const table of generateRandomTables([10, 20, 30])) {

    const io = ArrowIOTestHelper.stream(table);
    const name = `[\n ${table.schema.fields.join(',\n ')}\n]`;

    const validate = (source: any) => { validateRecordBatchReader('stream', 3, RecordBatchReader.from(source)); };
    const validateAsync = async (source: any) => { await validateAsyncRecordBatchReader('stream', 3, await RecordBatchReader.from(source)); };
    const validateAsyncWrapped = async (source: any) => { await validateAsyncRecordBatchReader('stream', 3, await RecordBatchReader.from(Promise.resolve(source))); };

    describe(`RecordBatchStreamReader (${name})`, () => {
        describe(`should read all RecordBatches`, () => {
            test(`Uint8Array`, io.buffer(validate));
            test(`Iterable`, io.iterable(validate));
        });
    });

    describe(`AsyncRecordBatchStreamReader (${name})`, () => {
        describe(`should read all RecordBatches`, () => {

            test('AsyncIterable', io.asyncIterable(validateAsync));
            test('fs.FileHandle', io.fsFileHandle(validateAsync));
            test('fs.ReadStream', io.fsReadableStream(validateAsync));
            test('stream.Readable', io.nodeReadableStream(validateAsync));
            test('whatwg.ReadableStream', io.whatwgReadableStream(validateAsync));
            test('whatwg.ReadableByteStream', io.whatwgReadableByteStream(validateAsync));

            test('Promise<AsyncIterable>', io.asyncIterable(validateAsyncWrapped));
            test('Promise<fs.FileHandle>', io.fsFileHandle(validateAsyncWrapped));
            test('Promise<fs.ReadStream>', io.fsReadableStream(validateAsyncWrapped));
            test('Promise<stream.Readable>', io.nodeReadableStream(validateAsyncWrapped));
            test('Promise<ReadableStream>', io.whatwgReadableStream(validateAsyncWrapped));
            test('Promise<ReadableByteStream>', io.whatwgReadableByteStream(validateAsyncWrapped));
        });
    });
}

describe(`RecordBatchStreamReader over message-sized chunks`, () => {

    const source = tableFromArrays({ x: [0, 10, 20, 30, 40, 50] });
    const batch = source.batches[0];
    const table = new Table(source.schema, [batch.slice(0, 2), batch.slice(2, 4), batch.slice(4, 6)]);

    test(`AsyncIterable yields each RecordBatch as soon as its chunk arrives`, async () => {
        const messages = [...ipcMessages(await RecordBatchStreamWriter.writeAll(table).toUint8Array())];
        let pulled = 0;
        async function* chunks() {
            let release: () => void;
            try {
                for (const message of messages) { pulled++; yield message; }
                // the byte stream stays open, like a WebSocket or network stream
                await new Promise<void>((resolve) => { release = resolve; });
            } finally {
                release!();
            }
        }

        const reader = await (await RecordBatchReader.from(chunks())).open();
        const iterator = reader[Symbol.asyncIterator]();

        // reading the schema should not pull the next chunk
        expect(pulled).toBe(1);
        for (let i = 0; i < table.batches.length; i++) {
            const { value } = await iterator.next();
            expect(value).toEqualRecordBatch(table.batches[i]);
            expect(pulled).toBe(i + 2);
        }
        await iterator.return!(null);
    });

    test(`AsyncIterable terminates when the byte stream ends`, async () => {
        const messages = [...ipcMessages(await RecordBatchStreamWriter.writeAll(table).toUint8Array())];
        const reader = await (await RecordBatchReader.from(async function* () { yield* messages; }())).open();
        let i = 0;
        for await (const batch of reader) {
            expect(batch).toEqualRecordBatch(table.batches[i++]);
        }
        expect(i).toBe(table.batches.length);
    });

    test(`Iterable yields each RecordBatch as soon as its chunk arrives`, () => {
        const messages = [...ipcMessages(RecordBatchStreamWriter.writeAll(table).toUint8Array(true))];
        let pulled = 0;
        function* chunks() {
            for (const message of messages) { pulled++; yield message; }
        }

        const reader = RecordBatchReader.from(chunks()).open();
        expect(pulled).toBe(1);

        let i = 0;
        for (const batch of reader) {
            expect(batch).toEqualRecordBatch(table.batches[i]);
            expect(pulled).toBe(++i + 1);
        }
        expect(i).toBe(table.batches.length);
    });

    if (process.env.TEST_DOM_STREAMS === 'true') {
        test(`whatwg.ReadableStream yields each RecordBatch as soon as its chunk arrives`, async () => {
            const messages = [...ipcMessages(await RecordBatchStreamWriter.writeAll(table).toUint8Array())];
            let pulled = 0;
            const stream = new ReadableStream<Uint8Array>({
                pull(controller) {
                    if (pulled < messages.length) {
                        controller.enqueue(messages[pulled++]);
                    } else {
                        controller.error(new Error(`source pulled past the last message`));
                    }
                }
            }, { highWaterMark: 0 });

            const reader = await (await RecordBatchReader.from(stream)).open();
            const iterator = reader[Symbol.asyncIterator]();

            // reading the schema should not pull the next chunk
            expect(pulled).toBe(1);
            for (let i = 0; i < table.batches.length; i++) {
                const { value } = await iterator.next();
                expect(value).toEqualRecordBatch(table.batches[i]);
                expect(pulled).toBe(i + 2);
            }
            await iterator.return!(null);
        });
    }

    if (process.env.TEST_NODE_STREAMS === 'true') {
        test(`stream.Readable yields each RecordBatch as soon as its chunk arrives`, async () => {
            const { PassThrough } = await import('node:stream');
            const messages = [...ipcMessages(await RecordBatchStreamWriter.writeAll(table).toUint8Array())];
            const stream = new PassThrough();
            stream.write(messages[0]);

            const reader = await (await RecordBatchReader.from(stream)).open();
            const iterator = reader[Symbol.asyncIterator]();

            for (let i = 0; i < table.batches.length; i++) {
                stream.write(messages[i + 1]);
                // each batch resolves without a pull of the next chunk
                const next = iterator.next();
                const settled = await Promise.race([
                    next.then(() => true),
                    new Promise((resolve) => setImmediate(() => resolve(false)))
                ]);
                expect(settled).toBe(true);
                expect((await next).value).toEqualRecordBatch(table.batches[i]);
            }
            await iterator.return!(null);
        });
    }
});

test.each([null, 0])(`ByteStream and AsyncByteStream advance after read(%s)`, async (size) => {
    const chunks = [new Uint8Array([1]), new Uint8Array([2])];
    for (const stream of [new ByteStream(chunks), new AsyncByteStream(async function* () { yield* chunks; }())]) {
        for (const chunk of chunks) {
            expect(await stream.read(size)).toEqual(chunk);
        }
        expect(await stream.read(size)).toEqual(new Uint8Array(0));
        expect((await stream.next(size)).done).toBe(true);
    }
});

// Splits an IPC stream into one Uint8Array per encapsulated message
// (4- or 8-byte prefix, flatbuffer metadata + padding, body, EOS).
function* ipcMessages(buffer: Uint8Array) {
    const view = new DataView(buffer.buffer, buffer.byteOffset, buffer.byteLength);
    for (let offset = 0; offset < buffer.byteLength;) {
        const prefixSize = view.getInt32(offset, true) === -1 ? 8 : 4;
        const metadataLength = view.getInt32(offset + (prefixSize - 4), true);
        const bodyLength = metadataLength === 0 ? 0 : Number(
            Message.decode(buffer.subarray(offset + prefixSize, offset + prefixSize + metadataLength)).bodyLength);
        yield buffer.subarray(offset, offset += prefixSize + metadataLength + bodyLength);
    }
}
