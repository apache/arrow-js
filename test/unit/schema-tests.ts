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

import '../jest-extensions.js';
import { Field, Int32, Schema } from 'apache-arrow';

describe('Field.withMetadata', () => {
    test('replaces metadata from plain objects', () => {
        const field = new Field('col', new Int32(), true, new Map([['foo', 'bar']]));
        const updated = field.withMetadata({ baz: 'qux' });
        expect(updated).not.toBe(field);
        expect(updated.metadata.get('foo')).toBeUndefined();
        expect(updated.metadata.get('baz')).toBe('qux');
    });

    test('replaces metadata from Maps', () => {
        const field = new Field('col', new Int32(), true, new Map([['foo', 'bar']]));
        const updated = field.withMetadata(new Map([['foo', 'baz']]));
        expect(updated.metadata.get('foo')).toBe('baz');
        expect(updated.metadata.size).toBe(1);
    });

    test('clears metadata when null is passed', () => {
        const field = new Field('col', new Int32(), true, new Map([['foo', 'bar']]));
        const updated = field.withMetadata(null);
        expect(updated.metadata.size).toBe(0);
    });
});

describe('Schema.withMetadata', () => {
    test('replaces metadata from plain objects', () => {
        const schema = new Schema([new Field('col', new Int32())], new Map([['foo', 'bar']]));
        const updated = schema.withMetadata({ baz: 'qux' });
        expect(updated).not.toBe(schema);
        expect(schema.metadata.get('baz')).toBeUndefined();
        expect(updated.metadata.get('foo')).toBeUndefined();
        expect(updated.metadata.get('baz')).toBe('qux');
    });

    test('replaces metadata from Maps', () => {
        const schema = new Schema([new Field('col', new Int32())], new Map([['foo', 'bar']]));
        const updated = schema.withMetadata(new Map([['foo', 'baz']]));
        expect(updated.metadata.get('foo')).toBe('baz');
        expect(updated.metadata.size).toBe(1);
    });

    test('clears metadata when null is passed', () => {
        const schema = new Schema([new Field('col', new Int32())], new Map([['foo', 'bar']]));
        const updated = schema.withMetadata(null);
        expect(updated.metadata.size).toBe(0);
    });
});
