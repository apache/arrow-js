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

// JSONVectorAssembler must serialize a vector the same way regardless of where
// its backing typed arrays sit in their buffers. For each affected type we take
// a freshly-generated Vector (byteOffset === 0), rewrap every backing typed
// array in a subarray view at byteOffset > 0, and assert both serialize to
// identical JSON.

import {
    Data, DataType, Field, RecordBatch, Schema, Struct,
} from 'apache-arrow';
import { JSONVectorAssembler } from 'apache-arrow/visitor/jsonvectorassembler';
import * as generate from '../../../generate-test-data.js';

/**
 * Return a copy of `arr` whose data lives at a nonzero byteOffset inside a
 * larger underlying buffer. Equivalent to the state of a typed array that
 * came out of `tableFromIPC` — same logical contents, shifted memory layout.
 */
function padBuffer<T extends ArrayBufferView>(arr: T | undefined): T | undefined {
    if (!arr) return arr;
    const Ctor = arr.constructor as new (n: number) => any;
    const padding = 4;
    const padded = new Ctor((arr as any).length + padding * 2);
    padded.set(arr, padding);
    return padded.subarray(padding, padding + (arr as any).length) as T;
}

/**
 * Clone a Data so every backing typed array sits at byteOffset > 0.
 * Recurses into children so nested types stay consistent.
 */
function withOffsetViews<T extends DataType>(d: Data<T>): Data<T> {
    const newBuffers = Array.from(d.buffers as unknown as Array<ArrayBufferView | undefined>, padBuffer);
    const newChildren = d.children.map((c) => withOffsetViews(c));
    return d.clone(d.type, d.offset, d.length, d.nullCount, newBuffers as any, newChildren);
}

/**
 * Wrap a Data in a single-field RecordBatch so we can hand it to assemble().
 */
function batchOf<T extends DataType>(type: T, data: Data<T>): RecordBatch<any> {
    const field = new Field('x', type, true);
    const schema = new Schema([field]);
    const structType = new Struct([field]);
    const root = data.clone(structType as any, 0, data.length, 0, [undefined, undefined, undefined, undefined] as any, [data]);
    return new RecordBatch(schema, root as Data<Struct>);
}

// Every affected callsite of bigNumsToStrings, one entry per type. Adding
// a new affected type is a one-line addition here.
const affectedTypeCases: Array<{ name: string; gen: () => generate.GeneratedVector<any> }> = [
    { name: 'Int64', gen: () => generate.int64() },
    { name: 'Uint64', gen: () => generate.uint64() },
    { name: 'DateMillisecond', gen: () => generate.dateMillisecond() },
    { name: 'TimestampSecond', gen: () => generate.timestampSecond() },
    { name: 'TimestampMillisecond', gen: () => generate.timestampMillisecond() },
    { name: 'TimestampMicrosecond', gen: () => generate.timestampMicrosecond() },
    { name: 'TimestampNanosecond', gen: () => generate.timestampNanosecond() },
    { name: 'TimeMicrosecond', gen: () => generate.timeMicrosecond() },
    { name: 'TimeNanosecond', gen: () => generate.timeNanosecond() },
    { name: 'DurationSecond', gen: () => generate.durationSecond() },
    { name: 'DurationMillisecond', gen: () => generate.durationMillisecond() },
    { name: 'DurationMicrosecond', gen: () => generate.durationMicrosecond() },
    { name: 'DurationNanosecond', gen: () => generate.durationNanosecond() },
    { name: 'Decimal', gen: () => generate.decimal() },
    { name: 'LargeUtf8', gen: () => generate.largeUtf8() },
    { name: 'LargeBinary', gen: () => generate.largeBinary() },
];

// JSONVectorAssembler is an internal (non-public) export. Single-bundle build
// targets such as the UMD bundle collapse every `apache-arrow/*` deep import to
// the public bundle, which doesn't re-export it.
// So, skip the test on UMD builds (covered there via the public API in
// json-writer-tests.ts).
if (!JSONVectorAssembler) {
    console.warn(
        'Skipping "JSONVectorAssembler offset-view safety" test under UMD build.'
    );
}
const describeAssembler = JSONVectorAssembler ? describe : describe.skip;

describeAssembler('JSONVectorAssembler offset-view safety', () => {
    for (const { name, gen } of affectedTypeCases) {
        test(name, () => {
            const { vector } = gen();
            const fresh = vector.data[0];
            const viewed = withOffsetViews(fresh);

            // Sanity: the rewrap really did shift at least one buffer
            // past byteOffset 0, so this run actually exercises the bug
            // precondition.
            const buffers = Array.from(viewed.buffers as unknown as Array<ArrayBufferView | undefined>);
            expect(buffers.some((b) => b && b.byteOffset > 0)).toBe(true);

            const [[jsonFresh]] = JSONVectorAssembler.assemble(batchOf(fresh.type, fresh));
            const [[jsonViewed]] = JSONVectorAssembler.assemble(batchOf(fresh.type, viewed));
            expect(jsonViewed).toEqual(jsonFresh);
        });
    }
});
