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

import { makeData } from 'apache-arrow/data';
import { BinaryView, Utf8View, ListView, LargeListView, Int32 } from 'apache-arrow/type';
import { Vector } from 'apache-arrow/vector';
import { Field } from 'apache-arrow/schema';

const BINARY_VIEW_SIZE = 16;

function createInlineView(value: Uint8Array) {
    const view = new Uint8Array(BINARY_VIEW_SIZE);
    const dv = new DataView(view.buffer, view.byteOffset, view.byteLength);
    dv.setInt32(0, value.length, true);
    view.set(value, 4);
    return view;
}

function createReferencedView(value: Uint8Array, bufferIndex: number, offset: number) {
    const view = new Uint8Array(BINARY_VIEW_SIZE);
    const dv = new DataView(view.buffer, view.byteOffset, view.byteLength);
    dv.setInt32(0, value.length, true);
    view.set(value.subarray(0, Math.min(4, value.length)), 4);
    dv.setInt32(8, bufferIndex, true);
    dv.setInt32(12, offset, true);
    return view;
}

describe('BinaryView and Utf8View integration', () => {
    const inlineBinary = new Uint8Array([1, 2, 3, 4, 5]);
    const referencedBinary = new Uint8Array(Array.from({ length: 20 }, (_, i) => i));
    const referencedUtf8 = 'View types are fun!';

    const inlineUtf8 = 'hi';

    const binaryViews = new Uint8Array(BINARY_VIEW_SIZE * 3);
    binaryViews.set(createInlineView(inlineBinary), 0);
    binaryViews.set(createReferencedView(referencedBinary, 0, 0), BINARY_VIEW_SIZE);
    binaryViews.set(createReferencedView(new Uint8Array(0), 0, referencedBinary.length), 2 * BINARY_VIEW_SIZE);

    const utf8Payload = new TextEncoder().encode(referencedUtf8);
    const utf8Views = new Uint8Array(BINARY_VIEW_SIZE * 2);
    utf8Views.set(createInlineView(new TextEncoder().encode(inlineUtf8)), 0);
    utf8Views.set(createReferencedView(utf8Payload, 0, 0), BINARY_VIEW_SIZE);

    const nullBitmap = new Uint8Array([0b00000011]);

    const binaryData = makeData({
        type: new BinaryView(),
        length: 3,
        nullBitmap,
        views: binaryViews,
        variadicBuffers: [referencedBinary]
    });

    const utf8Data = makeData({
        type: new Utf8View(),
        length: 2,
        nullBitmap: new Uint8Array([0b00000011]),
        views: utf8Views,
        variadicBuffers: [utf8Payload]
    });

    const listChildData = makeData({
        type: new Int32(),
        length: 5,
        nullCount: 0,
        data: new Int32Array([10, 11, 30, 31, 32])
    });

    const listViewData = makeData({
        type: new ListView(new Field('item', new Int32())),
        length: 3,
        nullCount: 1,
        nullBitmap: new Uint8Array([0b00000101]),
        valueOffsets: new Int32Array([0, 2, 2]),
        valueSizes: new Int32Array([2, 0, 3]),
        child: listChildData
    });

    const largeChildData = makeData({
        type: new Int32(),
        length: 4,
        nullCount: 0,
        data: new Int32Array([100, 200, 300, 400])
    });

    const largeListViewData = makeData({
        type: new LargeListView(new Field('item', new Int32())),
        length: 3,
        nullBitmap: new Uint8Array([0b00000111]),
        valueOffsets: BigInt64Array.from([0n, 1n, 3n]),
        valueSizes: BigInt64Array.from([1n, 2n, 1n]),
        child: largeChildData
    });

    it('reads BinaryView values via Vector', () => {
        const vector = new Vector([binaryData]);
        expect(vector.get(0)).toEqual(inlineBinary);
        expect(vector.get(1)).toEqual(referencedBinary);
        expect(vector.get(2)).toBeNull();
    });

    it('reads Utf8View values via Vector', () => {
        const vector = new Vector([utf8Data]);
        expect(vector.get(0)).toBe(inlineUtf8);
        expect(vector.get(1)).toBe(referencedUtf8);
    });

    it('reads ListView values via Vector', () => {
        const vector = new Vector([listViewData]);
        expect(vector.get(0)?.toJSON()).toEqual([10, 11]);
        expect(vector.get(1)).toBeNull();
        expect(vector.get(2)?.toJSON()).toEqual([30, 31, 32]);
    });

    it('reads LargeListView values via Vector', () => {
        const vector = new Vector([largeListViewData]);
        expect(vector.get(0)?.toJSON()).toEqual([100]);
        expect(vector.get(1)?.toJSON()).toEqual([200, 300]);
        expect(vector.get(2)?.toJSON()).toEqual([400]);
    });

});
