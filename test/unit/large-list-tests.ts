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
    Field, Int32, LargeList, Vector, makeData,
} from 'apache-arrow';
import { VectorLoader } from 'apache-arrow/visitor/vectorloader';
import { FieldNode, BufferRegion } from 'apache-arrow/ipc/metadata/message';
import { describeIfExposed } from './utils.js';

describe('LargeList overflow semantics', () => {

    const buildChild = (length: number) => {
        const values = new Int32Array(length);
        for (let i = 0; i < length; i++) values[i] = i;
        return makeData({ type: new Int32(), length, data: values });
    };

    test(`.get() throws when an offset exceeds Number.MAX_SAFE_INTEGER`, () => {
        // Hand-build a LargeList Data whose second offset overflows the JS safe-integer range.
        // We can't actually allocate that much child data, so we synthesize a Data with a
        // small child but offsets that point past 2^53 — this exercises the bigIntToNumber
        // guard inside getList, the boundary where bigint offsets are narrowed to numbers.
        // makeData is a trusted in-memory constructor and performs no rebasing; the loader,
        // by contrast, rebases such offsets on read (see the wire-load tests below).
        const child = buildChild(8);
        const type = new LargeList<Int32>(new Field('item', new Int32(), true));
        const valueOffsets = BigInt64Array.from([0n, BigInt(Number.MAX_SAFE_INTEGER) + 1n]);
        const data = makeData({ type, length: 1, nullCount: 0, valueOffsets, child });
        const vec = new Vector([data]);
        expect(() => vec.get(0)).toThrow(TypeError);
    });

    test(`.get() works at the Number.MAX_SAFE_INTEGER boundary`, () => {
        // Offset exactly at MAX_SAFE_INTEGER must not throw — only past it.
        const child = buildChild(8);
        const type = new LargeList<Int32>(new Field('item', new Int32(), true));
        const safeMax = BigInt(Number.MAX_SAFE_INTEGER);
        const valueOffsets = BigInt64Array.from([0n, safeMax]);
        const data = makeData({ type, length: 1, nullCount: 0, valueOffsets, child });
        const vec = new Vector([data]);
        // The conversion itself must succeed; the resulting slice is degenerate
        // because the child is small, but that's fine — we're verifying no throw.
        expect(() => vec.get(0)).not.toThrow();
    });
});

// VectorLoader/FieldNode/BufferRegion are internal exports, absent from
// single-bundle targets like the UMD bundle. Skip this test under such targets.
describeIfExposed('LargeList wire-format offset loading', VectorLoader, FieldNode, BufferRegion)(() => {

    // Build an IPC body + buffer/node metadata for a single-row LargeList<Int32>
    // with the given offsets and child values, then load it through VectorLoader.
    const loadSingleRow = (offsets: BigInt64Array, childValues: Int32Array) => {
        const type = new LargeList<Int32>(new Field('item', new Int32(), true));
        const offsetsBytes = new Uint8Array(offsets.buffer, offsets.byteOffset, offsets.byteLength);
        const childBytes = new Uint8Array(childValues.buffer, childValues.byteOffset, childValues.byteLength);
        const body = new Uint8Array(offsetsBytes.byteLength + childBytes.byteLength);
        body.set(offsetsBytes, 0);
        body.set(childBytes, offsetsBytes.byteLength);

        // Buffer regions: parent null bitmap (empty), offsets, child null bitmap (empty), child data.
        const buffers = [
            new BufferRegion(0, 0),
            new BufferRegion(0, offsetsBytes.byteLength),
            new BufferRegion(0, 0),
            new BufferRegion(offsetsBytes.byteLength, childBytes.byteLength),
        ];
        const nodes = [new FieldNode(1, 0), new FieldNode(childValues.length, 0)];
        const data = new VectorLoader(body, nodes, buffers, new Map()).visit(type);
        return { data, body };
    };

    test(`reads offsets as a zero-copy view in the common case`, () => {
        // Offsets within the safe-integer range must not be rebased or copied — the
        // loaded offsets buffer should be a view over the same ArrayBuffer as the body.
        const offsets = BigInt64Array.from([0n, 8n]);
        const childValues = Int32Array.from([10, 11, 12, 13, 14, 15, 16, 17]);
        const { data, body } = loadSingleRow(offsets, childValues);

        expect((data.valueOffsets as BigInt64Array).buffer).toBe(body.buffer);
        const vec = new Vector([data]);
        expect([...vec.get(0)!.toArray()]).toEqual([10, 11, 12, 13, 14, 15, 16, 17]);
    });

    test(`rebases absolute offsets past 2^53 on load`, () => {
        // A sliced view serialized with absolute (non-rebased) offsets puts the offset
        // values past Number.MAX_SAFE_INTEGER while the referenced span stays small. The
        // loader must rebase to 0 so the offsets remain narrowable; this is the one case
        // that copies. The Arrow spec permits either form on the wire.
        const base = BigInt(Number.MAX_SAFE_INTEGER) + 1n;
        const offsets = BigInt64Array.from([base, base + 8n]);
        const childValues = Int32Array.from([10, 11, 12, 13, 14, 15, 16, 17]);
        const { data, body } = loadSingleRow(offsets, childValues);

        // Rebased offsets are a fresh buffer, not a view over the body.
        expect((data.valueOffsets as BigInt64Array).buffer).not.toBe(body.buffer);
        const vec = new Vector([data]);
        expect(() => vec.get(0)).not.toThrow();
        expect([...vec.get(0)!.toArray()]).toEqual([10, 11, 12, 13, 14, 15, 16, 17]);
    });
});
