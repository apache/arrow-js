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

import '../../../jest-extensions.js';
import {
    Field,
    Float64,
    Int32,
    makeVector,
    RecordBatch,
    RecordBatchStreamWriter,
    Schema,
    Table,
} from 'apache-arrow';

describe('RecordBatchStreamWriter schema mismatch behavior', () => {

    const schemaA = new Schema([
        new Field('a', new Int32()),
    ]);

    const schemaB = new Schema([
        new Field('x', new Float64()),
    ]);

    function makeBatchA(): RecordBatch {
        return new Table({ a: makeVector(new Int32Array([1, 2, 3])) }).batches[0];
    }

    function makeBatchB(): RecordBatch {
        return new Table({ x: makeVector(new Float64Array([1.1, 2.2])) }).batches[0];
    }

    test('autoDestroy=true (default): writing a mismatched batch throws', () => {
        const writer = new RecordBatchStreamWriter();

        writer.write(makeBatchA());

        expect(() => writer.write(makeBatchB())).toThrow(
            'RecordBatch schema does not match the writer\'s schema.'
        );
    });

    test('error message includes expected and received schemas', () => {
        const writer = new RecordBatchStreamWriter();

        writer.write(makeBatchA());

        expect(() => writer.write(makeBatchB())).toThrow(/Expected:.*\n.*Received:/);
    });
});
