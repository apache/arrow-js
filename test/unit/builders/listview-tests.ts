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

import { ListView, LargeListView, Int32 } from '../../../src/type.js';
import { Field } from '../../../src/schema.js';
import { ListViewBuilder, LargeListViewBuilder } from '../../../src/builder/listview.js';
import { Int32Builder } from '../../../src/builder/int.js';
import { Vector } from '../../../src/vector.js';

describe('ListViewBuilder', () => {
    it('should build ListView with basic values', () => {
        const type = new ListView(new Field('item', new Int32()));
        const builder = new ListViewBuilder({ type });
        builder.addChild(new Int32Builder({ type: new Int32() }), 'item');

        builder.append([1, 2, 3]);
        builder.append([4, 5]);
        builder.append([6]);

        const vector = builder.finish().toVector();

        expect(vector).toHaveLength(3);
        expect(vector.get(0)?.toArray()).toEqual(new Int32Array([1, 2, 3]));
        expect(vector.get(1)?.toArray()).toEqual(new Int32Array([4, 5]));
        expect(vector.get(2)?.toArray()).toEqual(new Int32Array([6]));
    });

    it('should handle null values', () => {
        const type = new ListView(new Field('item', new Int32()));
        const builder = new ListViewBuilder({ type, nullValues: [null] });
        builder.addChild(new Int32Builder({ type: new Int32() }), 'item');

        builder.append([1, 2]);
        builder.append(null);
        builder.append([3, 4, 5]);

        const vector = builder.finish().toVector();

        expect(vector).toHaveLength(3);
        expect(vector.get(0)?.toArray()).toEqual(new Int32Array([1, 2]));
        expect(vector.get(1)).toBeNull();
        expect(vector.get(2)?.toArray()).toEqual(new Int32Array([3, 4, 5]));
    });

    it('should handle empty lists', () => {
        const type = new ListView(new Field('item', new Int32()));
        const builder = new ListViewBuilder({ type });
        builder.addChild(new Int32Builder({ type: new Int32() }), 'item');

        builder.append([]);
        builder.append([1, 2]);
        builder.append([]);

        const vector = builder.finish().toVector();

        expect(vector).toHaveLength(3);
        expect(vector.get(0)?.toArray()).toEqual(new Int32Array([]));
        expect(vector.get(1)?.toArray()).toEqual(new Int32Array([1, 2]));
        expect(vector.get(2)?.toArray()).toEqual(new Int32Array([]));
    });

    it('should handle multiple flushes', () => {
        const type = new ListView(new Field('item', new Int32()));
        const builder = new ListViewBuilder({ type });
        builder.addChild(new Int32Builder({ type: new Int32() }), 'item');

        builder.append([1, 2]);
        const data1 = builder.flush();
        builder.append([3, 4]);
        const data2 = builder.flush();

        builder.finish();

        const vector1 = new Vector([data1]);
        const vector2 = new Vector([data2]);

        expect(vector1).toHaveLength(1);
        expect(vector1.get(0)?.toArray()).toEqual(new Int32Array([1, 2]));
        expect(vector2).toHaveLength(1);
        expect(vector2.get(0)?.toArray()).toEqual(new Int32Array([3, 4]));
    });

    it('should build ListView with varying list sizes', () => {
        const type = new ListView(new Field('item', new Int32()));
        const builder = new ListViewBuilder({ type });
        builder.addChild(new Int32Builder({ type: new Int32() }), 'item');

        builder.append([1]);
        builder.append([2, 3]);
        builder.append([4, 5, 6]);
        builder.append([7, 8, 9, 10]);

        const vector = builder.finish().toVector();

        expect(vector).toHaveLength(4);
        expect(vector.get(0)?.toArray()).toEqual(new Int32Array([1]));
        expect(vector.get(1)?.toArray()).toEqual(new Int32Array([2, 3]));
        expect(vector.get(2)?.toArray()).toEqual(new Int32Array([4, 5, 6]));
        expect(vector.get(3)?.toArray()).toEqual(new Int32Array([7, 8, 9, 10]));
    });
});

describe('LargeListViewBuilder', () => {
    it('should build LargeListView with basic values', () => {
        const type = new LargeListView(new Field('item', new Int32()));
        const builder = new LargeListViewBuilder({ type });
        builder.addChild(new Int32Builder({ type: new Int32() }), 'item');

        builder.append([1, 2, 3]);
        builder.append([4, 5]);
        builder.append([6]);

        const vector = builder.finish().toVector();

        expect(vector).toHaveLength(3);
        expect(vector.get(0)?.toArray()).toEqual(new Int32Array([1, 2, 3]));
        expect(vector.get(1)?.toArray()).toEqual(new Int32Array([4, 5]));
        expect(vector.get(2)?.toArray()).toEqual(new Int32Array([6]));
    });

    it('should handle null values', () => {
        const type = new LargeListView(new Field('item', new Int32()));
        const builder = new LargeListViewBuilder({ type, nullValues: [null] });
        builder.addChild(new Int32Builder({ type: new Int32() }), 'item');

        builder.append([1, 2]);
        builder.append(null);
        builder.append([3, 4, 5]);

        const vector = builder.finish().toVector();

        expect(vector).toHaveLength(3);
        expect(vector.get(0)?.toArray()).toEqual(new Int32Array([1, 2]));
        expect(vector.get(1)).toBeNull();
        expect(vector.get(2)?.toArray()).toEqual(new Int32Array([3, 4, 5]));
    });

    it('should handle empty lists', () => {
        const type = new LargeListView(new Field('item', new Int32()));
        const builder = new LargeListViewBuilder({ type });
        builder.addChild(new Int32Builder({ type: new Int32() }), 'item');

        builder.append([]);
        builder.append([1, 2]);
        builder.append([]);

        const vector = builder.finish().toVector();

        expect(vector).toHaveLength(3);
        expect(vector.get(0)?.toArray()).toEqual(new Int32Array([]));
        expect(vector.get(1)?.toArray()).toEqual(new Int32Array([1, 2]));
        expect(vector.get(2)?.toArray()).toEqual(new Int32Array([]));
    });

    it('should use BigInt offsets internally', () => {
        const type = new LargeListView(new Field('item', new Int32()));
        const builder = new LargeListViewBuilder({ type });
        builder.addChild(new Int32Builder({ type: new Int32() }), 'item');

        builder.append([1, 2]);
        builder.append([3, 4, 5]);

        const data = builder.finish().flush();

        // Verify that offsets and sizes are BigInt64Array
        expect(data.valueOffsets).toBeInstanceOf(BigInt64Array);
        expect(data.values).toBeInstanceOf(BigInt64Array); // sizes buffer
    });
});

describe('ListView type properties', () => {
    it('should correctly report type name', () => {
        const type = new ListView(new Field('item', new Int32()));
        const builder = new ListViewBuilder({ type });
        builder.addChild(new Int32Builder({ type: new Int32() }), 'item');
        expect(builder.type.toString()).toBe('ListView<Int32>');
    });

    it('should correctly report LargeListView type name', () => {
        const type = new LargeListView(new Field('item', new Int32()));
        const builder = new LargeListViewBuilder({ type });
        builder.addChild(new Int32Builder({ type: new Int32() }), 'item');
        expect(builder.type.toString()).toBe('LargeListView<Int32>');
    });
});
