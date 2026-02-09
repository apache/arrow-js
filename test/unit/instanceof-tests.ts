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
    Schema, Field, DataType, Data, Vector, RecordBatch, Table,
    Int32, Utf8, Float64,
    makeData, vectorFromArray,
    isArrowSchema, isArrowField, isArrowDataType, isArrowData,
    isArrowVector, isArrowRecordBatch, isArrowTable
} from 'apache-arrow';

/**
 * Tests for Symbol.hasInstance implementation that enables instanceof
 * to work across different instances of the Arrow library.
 * 
 * @see https://github.com/apache/arrow/issues/61
 */
describe('Cross-library instanceof support', () => {

    describe('Schema', () => {
        const schema = new Schema([
            new Field('id', new Int32()),
            new Field('name', new Utf8())
        ]);

        test('instanceof works with Schema', () => {
            expect(schema instanceof Schema).toBe(true);
        });

        test('Schema.isSchema() returns true for Schema instances', () => {
            expect(Schema.isSchema(schema)).toBe(true);
        });

        test('isArrowSchema() returns true for Schema instances', () => {
            expect(isArrowSchema(schema)).toBe(true);
        });

        test('Schema.isSchema() returns false for non-Schema values', () => {
            expect(Schema.isSchema(null)).toBe(false);
            expect(Schema.isSchema(undefined)).toBe(false);
            expect(Schema.isSchema({})).toBe(false);
            expect(Schema.isSchema({ fields: [] })).toBe(false);
            expect(Schema.isSchema('Schema')).toBe(false);
        });

        test('isArrowSchema() returns false for non-Schema values', () => {
            expect(isArrowSchema(null)).toBe(false);
            expect(isArrowSchema(undefined)).toBe(false);
            expect(isArrowSchema({})).toBe(false);
        });
    });

    describe('Field', () => {
        const field = new Field('test', new Int32());

        test('instanceof works with Field', () => {
            expect(field instanceof Field).toBe(true);
        });

        test('Field.isField() returns true for Field instances', () => {
            expect(Field.isField(field)).toBe(true);
        });

        test('isArrowField() returns true for Field instances', () => {
            expect(isArrowField(field)).toBe(true);
        });

        test('Field.isField() returns false for non-Field values', () => {
            expect(Field.isField(null)).toBe(false);
            expect(Field.isField(undefined)).toBe(false);
            expect(Field.isField({})).toBe(false);
            expect(Field.isField({ name: 'test', type: new Int32() })).toBe(false);
        });

        test('isArrowField() returns false for non-Field values', () => {
            expect(isArrowField(null)).toBe(false);
            expect(isArrowField(undefined)).toBe(false);
            expect(isArrowField({})).toBe(false);
        });
    });

    describe('DataType', () => {
        const dataType = new Int32();

        test('instanceof works with DataType', () => {
            expect(dataType instanceof DataType).toBe(true);
        });

        test('DataType.isDataType() returns true for DataType instances', () => {
            expect(DataType.isDataType(dataType)).toBe(true);
            expect(DataType.isDataType(new Utf8())).toBe(true);
            expect(DataType.isDataType(new Float64())).toBe(true);
        });

        test('isArrowDataType() returns true for DataType instances', () => {
            expect(isArrowDataType(dataType)).toBe(true);
        });

        test('DataType.isDataType() returns false for non-DataType values', () => {
            expect(DataType.isDataType(null)).toBe(false);
            expect(DataType.isDataType(undefined)).toBe(false);
            expect(DataType.isDataType({})).toBe(false);
            expect(DataType.isDataType({ typeId: 0 })).toBe(false);
        });

        test('isArrowDataType() returns false for non-DataType values', () => {
            expect(isArrowDataType(null)).toBe(false);
            expect(isArrowDataType(undefined)).toBe(false);
            expect(isArrowDataType({})).toBe(false);
        });
    });

    describe('Data', () => {
        const data = makeData({ type: new Int32(), length: 5 });

        test('instanceof works with Data', () => {
            expect(data instanceof Data).toBe(true);
        });

        test('Data.isData() returns true for Data instances', () => {
            expect(Data.isData(data)).toBe(true);
        });

        test('isArrowData() returns true for Data instances', () => {
            expect(isArrowData(data)).toBe(true);
        });

        test('Data.isData() returns false for non-Data values', () => {
            expect(Data.isData(null)).toBe(false);
            expect(Data.isData(undefined)).toBe(false);
            expect(Data.isData({})).toBe(false);
            expect(Data.isData({ type: new Int32(), length: 5 })).toBe(false);
        });

        test('isArrowData() returns false for non-Data values', () => {
            expect(isArrowData(null)).toBe(false);
            expect(isArrowData(undefined)).toBe(false);
            expect(isArrowData({})).toBe(false);
        });
    });

    describe('Vector', () => {
        const vector = vectorFromArray([1, 2, 3, 4, 5]);

        test('instanceof works with Vector', () => {
            expect(vector instanceof Vector).toBe(true);
        });

        test('Vector.isVector() returns true for Vector instances', () => {
            expect(Vector.isVector(vector)).toBe(true);
        });

        test('isArrowVector() returns true for Vector instances', () => {
            expect(isArrowVector(vector)).toBe(true);
        });

        test('Vector.isVector() returns false for non-Vector values', () => {
            expect(Vector.isVector(null)).toBe(false);
            expect(Vector.isVector(undefined)).toBe(false);
            expect(Vector.isVector({})).toBe(false);
            expect(Vector.isVector([1, 2, 3])).toBe(false);
        });

        test('isArrowVector() returns false for non-Vector values', () => {
            expect(isArrowVector(null)).toBe(false);
            expect(isArrowVector(undefined)).toBe(false);
            expect(isArrowVector({})).toBe(false);
        });
    });

    describe('RecordBatch', () => {
        const schema = new Schema([
            new Field('id', new Int32()),
            new Field('value', new Float64())
        ]);
        const batch = new RecordBatch({
            id: vectorFromArray([1, 2, 3]).data[0],
            value: vectorFromArray([1.1, 2.2, 3.3]).data[0]
        });

        test('instanceof works with RecordBatch', () => {
            expect(batch instanceof RecordBatch).toBe(true);
        });

        test('RecordBatch.isRecordBatch() returns true for RecordBatch instances', () => {
            expect(RecordBatch.isRecordBatch(batch)).toBe(true);
        });

        test('isArrowRecordBatch() returns true for RecordBatch instances', () => {
            expect(isArrowRecordBatch(batch)).toBe(true);
        });

        test('RecordBatch.isRecordBatch() returns false for non-RecordBatch values', () => {
            expect(RecordBatch.isRecordBatch(null)).toBe(false);
            expect(RecordBatch.isRecordBatch(undefined)).toBe(false);
            expect(RecordBatch.isRecordBatch({})).toBe(false);
            expect(RecordBatch.isRecordBatch({ schema, numRows: 3 })).toBe(false);
        });

        test('isArrowRecordBatch() returns false for non-RecordBatch values', () => {
            expect(isArrowRecordBatch(null)).toBe(false);
            expect(isArrowRecordBatch(undefined)).toBe(false);
            expect(isArrowRecordBatch({})).toBe(false);
        });
    });

    describe('Table', () => {
        const table = new Table({
            id: vectorFromArray([1, 2, 3]),
            name: vectorFromArray(['a', 'b', 'c'])
        });

        test('instanceof works with Table', () => {
            expect(table instanceof Table).toBe(true);
        });

        test('Table.isTable() returns true for Table instances', () => {
            expect(Table.isTable(table)).toBe(true);
        });

        test('isArrowTable() returns true for Table instances', () => {
            expect(isArrowTable(table)).toBe(true);
        });

        test('Table.isTable() returns false for non-Table values', () => {
            expect(Table.isTable(null)).toBe(false);
            expect(Table.isTable(undefined)).toBe(false);
            expect(Table.isTable({})).toBe(false);
            expect(Table.isTable({ schema: new Schema([]), batches: [] })).toBe(false);
        });

        test('isArrowTable() returns false for non-Table values', () => {
            expect(isArrowTable(null)).toBe(false);
            expect(isArrowTable(undefined)).toBe(false);
            expect(isArrowTable({})).toBe(false);
        });
    });

    describe('Symbol.for markers', () => {
        test('Schema has the correct symbol marker', () => {
            const schema = new Schema([]);
            const marker = Symbol.for('apache-arrow/Schema');
            expect((schema as any)[marker]).toBe(true);
        });

        test('Field has the correct symbol marker', () => {
            const field = new Field('test', new Int32());
            const marker = Symbol.for('apache-arrow/Field');
            expect((field as any)[marker]).toBe(true);
        });

        test('DataType has the correct symbol marker', () => {
            const dataType = new Int32();
            const marker = Symbol.for('apache-arrow/DataType');
            expect((dataType as any)[marker]).toBe(true);
        });

        test('Data has the correct symbol marker', () => {
            const data = makeData({ type: new Int32(), length: 5 });
            const marker = Symbol.for('apache-arrow/Data');
            expect((data as any)[marker]).toBe(true);
        });

        test('Vector has the correct symbol marker', () => {
            const vector = vectorFromArray([1, 2, 3]);
            const marker = Symbol.for('apache-arrow/Vector');
            expect((vector as any)[marker]).toBe(true);
        });

        test('RecordBatch has the correct symbol marker', () => {
            const batch = new RecordBatch({
                id: vectorFromArray([1, 2, 3]).data[0]
            });
            const marker = Symbol.for('apache-arrow/RecordBatch');
            expect((batch as any)[marker]).toBe(true);
        });

        test('Table has the correct symbol marker', () => {
            const table = new Table({ id: vectorFromArray([1, 2, 3]) });
            const marker = Symbol.for('apache-arrow/Table');
            expect((table as any)[marker]).toBe(true);
        });
    });

    describe('Cross-instance detection simulation', () => {
        /**
         * Simulates what happens when an object comes from a different
         * Arrow library instance. We create a plain object with the
         * Symbol.for marker to simulate this scenario.
         */
        test('Schema marker is detected on foreign objects', () => {
            const foreignSchema = {
                [Symbol.for('apache-arrow/Schema')]: true,
                fields: [],
                metadata: new Map()
            };
            expect(Schema.isSchema(foreignSchema)).toBe(true);
            expect(isArrowSchema(foreignSchema)).toBe(true);
        });

        test('Field marker is detected on foreign objects', () => {
            const foreignField = {
                [Symbol.for('apache-arrow/Field')]: true,
                name: 'test',
                type: new Int32()
            };
            expect(Field.isField(foreignField)).toBe(true);
            expect(isArrowField(foreignField)).toBe(true);
        });

        test('DataType marker is detected on foreign objects', () => {
            const foreignDataType = {
                [Symbol.for('apache-arrow/DataType')]: true,
                typeId: 8 // Int32
            };
            expect(DataType.isDataType(foreignDataType)).toBe(true);
            expect(isArrowDataType(foreignDataType)).toBe(true);
        });

        test('Data marker is detected on foreign objects', () => {
            const foreignData = {
                [Symbol.for('apache-arrow/Data')]: true,
                type: new Int32(),
                length: 5
            };
            expect(Data.isData(foreignData)).toBe(true);
            expect(isArrowData(foreignData)).toBe(true);
        });

        test('Vector marker is detected on foreign objects', () => {
            const foreignVector = {
                [Symbol.for('apache-arrow/Vector')]: true,
                data: [],
                type: new Int32()
            };
            expect(Vector.isVector(foreignVector)).toBe(true);
            expect(isArrowVector(foreignVector)).toBe(true);
        });

        test('RecordBatch marker is detected on foreign objects', () => {
            const foreignBatch = {
                [Symbol.for('apache-arrow/RecordBatch')]: true,
                schema: new Schema([]),
                numRows: 0
            };
            expect(RecordBatch.isRecordBatch(foreignBatch)).toBe(true);
            expect(isArrowRecordBatch(foreignBatch)).toBe(true);
        });

        test('Table marker is detected on foreign objects', () => {
            const foreignTable = {
                [Symbol.for('apache-arrow/Table')]: true,
                schema: new Schema([]),
                batches: []
            };
            expect(Table.isTable(foreignTable)).toBe(true);
            expect(isArrowTable(foreignTable)).toBe(true);
        });
    });
});
