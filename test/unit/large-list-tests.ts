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
        // guard inside getList, which is the boundary where bigint offsets are narrowed.
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
