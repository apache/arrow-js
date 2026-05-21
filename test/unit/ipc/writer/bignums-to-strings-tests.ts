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

// Focused unit tests for bigNumsToStrings(), the helper every 64-/128-bit
// JSON callsite (Int64, Uint64, Date, Timestamp, Time, Duration, Decimal,
// LargeUtf8/Binary/List offsets) routes through. The end-to-end matrix in
// json-offset-views-tests.ts exercises it through JSONVectorAssembler; these
// tests pin the helper's contract directly:
//
//   - it reinterprets the backing buffer as little-endian u32 words and groups
//     `stride` words per emitted value,
//   - it always formats UNSIGNED (BN.new(words, false)), so a signed input's
//     negative values come out as their two's-complement 64-bit magnitude,
//   - it honours values.byteOffset/byteLength rather than the whole buffer
//     (the regression fixed in "Respect typed-array byteOffset in
//     bigNumsToStrings"): reverting that fix breaks the offset-view cases here.

import { bigNumsToStrings } from 'apache-arrow/visitor/jsonvectorassembler';

// JSONVectorAssembler's module is an internal (non-public) deep import. UMD /
// single-bundle build targets collapse every `apache-arrow/*` path onto the
// public bundle, which does not re-export this helper. Skip there; the same
// behaviour is covered via the public API in json-writer-tests.ts.
const describeHelper = bigNumsToStrings ? describe : describe.skip;
if (!bigNumsToStrings) {
    console.warn('Skipping "bigNumsToStrings" unit test under UMD build.');
}

/**
 * Place `arr`'s contents at a nonzero byteOffset inside a larger buffer and
 * return a subarray view over them — the memory layout a typed array has after
 * coming out of tableFromIPC or a slice.
 */
function atOffset<T extends { length: number; set(a: any, o: number): void; subarray(a: number, b: number): T }>(
    Ctor: new (n: number) => T,
    values: ArrayLike<any>,
): T {
    const pad = 3;
    const backing = new Ctor(values.length + pad * 2);
    backing.set(values as any, pad);
    return backing.subarray(pad, pad + values.length);
}

describeHelper('bigNumsToStrings', () => {
    test('formats unsigned 64-bit values (stride 2)', () => {
        const values = [0n, 1n, 42n, 0xFFFFFFFFn, 0x1_0000_0000n, 1234567890123456789n, 0xFFFFFFFFFFFFFFFFn];
        const arr = BigUint64Array.from(values);
        expect([...bigNumsToStrings(arr, 2)]).toEqual(values.map(String));
    });

    test('formats signed 64-bit values as their unsigned two\'s-complement (stride 2)', () => {
        const values = [-1n, -2n, -1234567890n, 5n];
        const arr = BigInt64Array.from(values);
        const expected = values.map((v) => String(BigInt.asUintN(64, v)));
        expect([...bigNumsToStrings(arr, 2)]).toEqual(expected);
        // sanity: -1 is the all-ones 64-bit word
        expect(expected[0]).toBe('18446744073709551615');
    });

    test('formats unsigned 128-bit values (stride 4)', () => {
        // little-endian u32 words: value = w0 + w1*2^32 + w2*2^64 + w3*2^96
        const one = [1, 0, 0, 0];
        const twoTo96 = [0, 0, 0, 1];
        const arr = Uint32Array.from([...one, ...twoTo96]);
        expect([...bigNumsToStrings(arr, 4)]).toEqual([
            '1',
            String(2n ** 96n),
        ]);
    });

    test('yields one string per group of `stride` words', () => {
        const arr = BigUint64Array.from([10n, 20n, 30n]);
        expect([...bigNumsToStrings(arr, 2)]).toHaveLength(3);
    });

    test('honours byteOffset for 64-bit views (regression)', () => {
        const values = [0n, 1n, 1234567890123456789n, 0xFFFFFFFFFFFFFFFFn];
        const view = atOffset(BigUint64Array, values);

        // precondition: the view really starts past the buffer origin
        expect(view.byteOffset).toBeGreaterThan(0);

        expect([...bigNumsToStrings(view, 2)]).toEqual(values.map(String));
    });

    test('offset view and fresh array agree for signed 64-bit', () => {
        const values = [-1n, 9223372036854775807n, -9223372036854775808n, 0n];
        const fresh = BigInt64Array.from(values);
        const view = atOffset(BigInt64Array, values);

        expect(view.byteOffset).toBeGreaterThan(0);
        expect([...bigNumsToStrings(view, 2)]).toEqual([...bigNumsToStrings(fresh, 2)]);
    });
});
