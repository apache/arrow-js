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
        const input = { baz: 'qux' };
        const updated = field.withMetadata(input);
        expect(updated).not.toBe(field);
        expect(updated.metadata.get('foo')).toBeUndefined();
        expect(updated.metadata.get('baz')).toBe('qux');
        expect(field.metadata.get('foo')).toBe('bar');
        expect(field.metadata.get('baz')).toBeUndefined();
        // Mutating the input after the call should not affect the result
        input.baz = 'changed';
        (input as any).newKey = 'newValue';
        expect(updated.metadata.get('baz')).toBe('qux');
        expect(updated.metadata.get('newKey')).toBeUndefined();
    });

    test('replaces metadata from Maps', () => {
        const field = new Field('col', new Int32(), true, new Map([['foo', 'bar']]));
        const input = new Map([['foo', 'baz']]);
        const updated = field.withMetadata(input);
        expect(updated.metadata.get('foo')).toBe('baz');
        expect(updated.metadata.size).toBe(1);
        expect(field.metadata.get('foo')).toBe('bar');
        expect(field.metadata.size).toBe(1);
        // Mutating the input after the call should not affect the result
        input.set('foo', 'qux');
        input.set('newKey', 'newValue');
        expect(updated.metadata.get('foo')).toBe('baz');
        expect(updated.metadata.get('newKey')).toBeUndefined();
        expect(updated.metadata.size).toBe(1);
    });

    test('clears metadata when null is passed', () => {
        const field = new Field('col', new Int32(), true, new Map([['foo', 'bar']]));
        const updated = field.withMetadata(null);
        expect(updated.metadata.size).toBe(0);
        expect(field.metadata.get('foo')).toBe('bar');
        expect(field.metadata.size).toBe(1);
    });
});

describe('Schema.withMetadata', () => {
    test('replaces metadata from plain objects', () => {
        const schema = new Schema([new Field('col', new Int32())], new Map([['foo', 'bar']]));
        const input = { baz: 'qux' };
        const updated = schema.withMetadata(input);
        expect(updated).not.toBe(schema);
        expect(schema.metadata.get('baz')).toBeUndefined();
        expect(schema.metadata.get('foo')).toBe('bar');
        expect(updated.metadata.get('foo')).toBeUndefined();
        expect(updated.metadata.get('baz')).toBe('qux');
        // Mutating the input after the call should not affect the result
        input.baz = 'changed';
        (input as any).newKey = 'newValue';
        expect(updated.metadata.get('baz')).toBe('qux');
        expect(updated.metadata.get('newKey')).toBeUndefined();
    });

    test('replaces metadata from Maps', () => {
        const schema = new Schema([new Field('col', new Int32())], new Map([['foo', 'bar']]));
        const input = new Map([['foo', 'baz']]);
        const updated = schema.withMetadata(input);
        expect(updated.metadata.get('foo')).toBe('baz');
        expect(updated.metadata.size).toBe(1);
        expect(schema.metadata.get('foo')).toBe('bar');
        expect(schema.metadata.size).toBe(1);
        // Mutating the input after the call should not affect the result
        input.set('foo', 'qux');
        input.set('newKey', 'newValue');
        expect(updated.metadata.get('foo')).toBe('baz');
        expect(updated.metadata.get('newKey')).toBeUndefined();
        expect(updated.metadata.size).toBe(1);
    });

    test('clears metadata when null is passed', () => {
        const schema = new Schema([new Field('col', new Int32())], new Map([['foo', 'bar']]));
        const updated = schema.withMetadata(null);
        expect(updated.metadata.size).toBe(0);
        expect(schema.metadata.get('foo')).toBe('bar');
        expect(schema.metadata.size).toBe(1);
    });
});
