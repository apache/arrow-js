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
import { ListView, LargeListView, Int8 } from 'apache-arrow/type';
import { Vector } from 'apache-arrow/vector';
import { Field } from 'apache-arrow/schema';

describe('ListView and LargeListView integration', () => {
    describe('ListView<Int8>', () => {
        // Test case from Arrow spec documentation:
        // [[12, -7, 25], null, [0, -127, 127, 50], []]
        it('reads ListView values with in-order offsets', () => {
            const childData = makeData({
                type: new Int8(),
                length: 7,
                nullCount: 0,
                data: new Int8Array([12, -7, 25, 0, -127, 127, 50])
            });

            const offsets = new Int32Array([0, 7, 3, 0]);
            const sizes = new Int32Array([3, 0, 4, 0]);
            const nullBitmap = new Uint8Array([0b00001101]); // bits: [1,0,1,1] = valid, null, valid, valid

            const listViewData = makeData({
                type: new ListView(new Field('item', new Int8())),
                length: 4,
                nullCount: 1,
                nullBitmap,
                valueOffsets: offsets,
                sizes,
                child: childData
            });

            const vector = new Vector([listViewData]);

            expect(vector.get(0)?.toArray()).toEqual(new Int8Array([12, -7, 25]));
            expect(vector.get(1)).toBeNull();
            expect(vector.get(2)?.toArray()).toEqual(new Int8Array([0, -127, 127, 50]));
            expect(vector.get(3)?.toArray()).toEqual(new Int8Array([]));
        });

        // Test case from Arrow spec showing out-of-order offsets and value sharing:
        // [[12, -7, 25], null, [0, -127, 127, 50], [], [50, 12]]
        it('reads ListView values with out-of-order offsets and value sharing', () => {
            const childData = makeData({
                type: new Int8(),
                length: 7,
                nullCount: 0,
                data: new Int8Array([0, -127, 127, 50, 12, -7, 25])
            });

            // Out of order offsets: [4, 7, 0, 0, 3]
            const offsets = new Int32Array([4, 7, 0, 0, 3]);
            const sizes = new Int32Array([3, 0, 4, 0, 2]);
            const nullBitmap = new Uint8Array([0b00011101]); // [1,0,1,1,1] = valid, null, valid, valid, valid

            const listViewData = makeData({
                type: new ListView(new Field('item', new Int8())),
                length: 5,
                nullCount: 1,
                nullBitmap,
                valueOffsets: offsets,
                sizes,
                child: childData
            });

            const vector = new Vector([listViewData]);

            // List 0: offset=4, size=3 -> [12, -7, 25]
            expect(vector.get(0)?.toArray()).toEqual(new Int8Array([12, -7, 25]));
            // List 1: null
            expect(vector.get(1)).toBeNull();
            // List 2: offset=0, size=4 -> [0, -127, 127, 50]
            expect(vector.get(2)?.toArray()).toEqual(new Int8Array([0, -127, 127, 50]));
            // List 3: offset=0, size=0 -> []
            expect(vector.get(3)?.toArray()).toEqual(new Int8Array([]));
            // List 4: offset=3, size=2 -> [50, 12] (shares values with list 2)
            expect(vector.get(4)?.toArray()).toEqual(new Int8Array([50, 12]));
        });

        it('handles all null ListView', () => {
            const childData = makeData({
                type: new Int8(),
                length: 0,
                nullCount: 0,
                data: new Int8Array([])
            });

            const offsets = new Int32Array([0, 0, 0]);
            const sizes = new Int32Array([0, 0, 0]);
            const nullBitmap = new Uint8Array([0b00000000]); // all null

            const listViewData = makeData({
                type: new ListView(new Field('item', new Int8())),
                length: 3,
                nullCount: 3,
                nullBitmap,
                valueOffsets: offsets,
                sizes,
                child: childData
            });

            const vector = new Vector([listViewData]);

            expect(vector.get(0)).toBeNull();
            expect(vector.get(1)).toBeNull();
            expect(vector.get(2)).toBeNull();
        });

        it('handles ListView with all empty lists', () => {
            const childData = makeData({
                type: new Int8(),
                length: 0,
                nullCount: 0,
                data: new Int8Array([])
            });

            const offsets = new Int32Array([0, 0, 0]);
            const sizes = new Int32Array([0, 0, 0]);
            const nullBitmap = new Uint8Array([0b00000111]); // all valid

            const listViewData = makeData({
                type: new ListView(new Field('item', new Int8())),
                length: 3,
                nullCount: 0,
                nullBitmap,
                valueOffsets: offsets,
                sizes,
                child: childData
            });

            const vector = new Vector([listViewData]);

            expect(vector.get(0)?.toArray()).toEqual(new Int8Array([]));
            expect(vector.get(1)?.toArray()).toEqual(new Int8Array([]));
            expect(vector.get(2)?.toArray()).toEqual(new Int8Array([]));
        });

        it('handles ListView with single element lists', () => {
            const childData = makeData({
                type: new Int8(),
                length: 3,
                nullCount: 0,
                data: new Int8Array([42, -1, 100])
            });

            const offsets = new Int32Array([0, 1, 2]);
            const sizes = new Int32Array([1, 1, 1]);
            const nullBitmap = new Uint8Array([0b00000111]);

            const listViewData = makeData({
                type: new ListView(new Field('item', new Int8())),
                length: 3,
                nullCount: 0,
                nullBitmap,
                valueOffsets: offsets,
                sizes,
                child: childData
            });

            const vector = new Vector([listViewData]);

            expect(vector.get(0)?.toArray()).toEqual(new Int8Array([42]));
            expect(vector.get(1)?.toArray()).toEqual(new Int8Array([-1]));
            expect(vector.get(2)?.toArray()).toEqual(new Int8Array([100]));
        });
    });

    describe('LargeListView<Int8>', () => {
        it('reads LargeListView values with BigInt offsets', () => {
            const childData = makeData({
                type: new Int8(),
                length: 7,
                nullCount: 0,
                data: new Int8Array([12, -7, 25, 0, -127, 127, 50])
            });

            const offsets = new BigInt64Array([0n, 7n, 3n, 0n]);
            const sizes = new BigInt64Array([3n, 0n, 4n, 0n]);
            const nullBitmap = new Uint8Array([0b00001101]);

            const largeListViewData = makeData({
                type: new LargeListView(new Field('item', new Int8())),
                length: 4,
                nullCount: 1,
                nullBitmap,
                valueOffsets: offsets,
                sizes,
                child: childData
            });

            const vector = new Vector([largeListViewData]);

            expect(vector.get(0)?.toArray()).toEqual(new Int8Array([12, -7, 25]));
            expect(vector.get(1)).toBeNull();
            expect(vector.get(2)?.toArray()).toEqual(new Int8Array([0, -127, 127, 50]));
            expect(vector.get(3)?.toArray()).toEqual(new Int8Array([]));
        });

        it('reads LargeListView with out-of-order offsets', () => {
            const childData = makeData({
                type: new Int8(),
                length: 5,
                nullCount: 0,
                data: new Int8Array([10, 20, 30, 40, 50])
            });

            // Out of order: list 0 starts at 2, list 1 starts at 0
            const offsets = new BigInt64Array([2n, 0n]);
            const sizes = new BigInt64Array([3n, 2n]);
            const nullBitmap = new Uint8Array([0b00000011]);

            const largeListViewData = makeData({
                type: new LargeListView(new Field('item', new Int8())),
                length: 2,
                nullCount: 0,
                nullBitmap,
                valueOffsets: offsets,
                sizes,
                child: childData
            });

            const vector = new Vector([largeListViewData]);

            expect(vector.get(0)?.toArray()).toEqual(new Int8Array([30, 40, 50]));
            expect(vector.get(1)?.toArray()).toEqual(new Int8Array([10, 20]));
        });
    });

    describe('ListView properties', () => {
        it('has correct type properties', () => {
            const listViewType = new ListView(new Field('item', new Int8()));
            expect(listViewType.typeId).toBe(25); // Type.ListView
            expect(listViewType.toString()).toBe('ListView<Int8>');
            expect(listViewType.valueType).toBeInstanceOf(Int8);
            expect(listViewType.valueField.name).toBe('item');
        });

        it('has correct type properties for LargeListView', () => {
            const largeListViewType = new LargeListView(new Field('item', new Int8()));
            expect(largeListViewType.typeId).toBe(26); // Type.LargeListView
            expect(largeListViewType.toString()).toBe('LargeListView<Int8>');
            expect(largeListViewType.valueType).toBeInstanceOf(Int8);
            expect(largeListViewType.valueField.name).toBe('item');
        });
    });
});
