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

import '../../jest-extensions.js';

import {
    makeData, makeVector,
    Schema, Field, RecordBatch,
    Struct, Int32
} from 'apache-arrow';

describe('Struct with zero children', () => {

    test('makeData with empty children preserves explicit length', () => {
        const data = makeData({ type: new Struct([]), length: 50, nullCount: 0, children: [] });
        expect(data).toHaveLength(50);
    });

    test('makeData with empty children and no length defaults to 0', () => {
        const data = makeData({ type: new Struct([]), children: [] });
        expect(data).toHaveLength(0);
    });

    test('setChildAt preserves numRows after column replacement', () => {
        const field = Field.new({ name: 'a', type: new Int32, nullable: true });
        const schema = new Schema([field]);
        const childData = makeData({ type: new Int32, length: 10, nullCount: 0 });
        const structData = makeData({
            type: new Struct(schema.fields),
            length: 10,
            nullCount: 0,
            children: [childData]
        });
        const batch = new RecordBatch(schema, structData);
        expect(batch.numRows).toBe(10);

        const newChild = makeVector(new Int32Array([1, 2, 3, 4, 5, 6, 7, 8, 9, 10]));
        const newBatch = batch.setChildAt(0, newChild);
        expect(newBatch.numRows).toBe(10);
    });
});
