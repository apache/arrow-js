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

import { BinaryView, Utf8View } from '../../../src/type.js';
import { makeBuilder, vectorFromArray } from '../../../src/factories.js';

describe('BinaryViewBuilder', () => {
    it('should build inline binary values (≤12 bytes)', () => {
        const builder = makeBuilder({ type: new BinaryView() });
        const values = [
            new Uint8Array([1, 2, 3]),
            new Uint8Array([4, 5, 6, 7, 8, 9, 10, 11, 12]),
            new Uint8Array([13])
        ];

        for (const value of values) {
            builder.append(value);
        }

        const vector = builder.finish().toVector();
        expect(vector).toHaveLength(3);
        expect(vector.get(0)).toEqual(values[0]);
        expect(vector.get(1)).toEqual(values[1]);
        expect(vector.get(2)).toEqual(values[2]);
    });

    it('should build out-of-line binary values (>12 bytes)', () => {
        const builder = makeBuilder({ type: new BinaryView() });
        const value = new Uint8Array(100);
        for (let i = 0; i < 100; i++) {
            value[i] = i % 256;
        }

        builder.append(value);

        const vector = builder.finish().toVector();
        expect(vector).toHaveLength(1);
        expect(vector.get(0)).toEqual(value);
    });

    it('should build mixed inline and out-of-line values', () => {
        const builder = makeBuilder({ type: new BinaryView() });
        const small = new Uint8Array([1, 2, 3]);
        const large = new Uint8Array(50);
        for (let i = 0; i < 50; i++) {
            large[i] = i % 256;
        }

        builder.append(small);
        builder.append(large);
        builder.append(small);

        const vector = builder.finish().toVector();
        expect(vector).toHaveLength(3);
        expect(vector.get(0)).toEqual(small);
        expect(vector.get(1)).toEqual(large);
        expect(vector.get(2)).toEqual(small);
    });

    it('should handle null values', () => {
        const builder = makeBuilder({ type: new BinaryView(), nullValues: [null] });

        builder.append(new Uint8Array([1, 2, 3]));
        builder.append(null);
        builder.append(new Uint8Array([4, 5, 6]));

        const vector = builder.finish().toVector();
        expect(vector).toHaveLength(3);
        expect(vector.get(0)).toEqual(new Uint8Array([1, 2, 3]));
        expect(vector.get(1)).toBeNull();
        expect(vector.get(2)).toEqual(new Uint8Array([4, 5, 6]));
    });

    it('should handle empty values', () => {
        const builder = makeBuilder({ type: new BinaryView() });

        builder.append(new Uint8Array([]));
        builder.append(new Uint8Array([1]));

        const vector = builder.finish().toVector();
        expect(vector).toHaveLength(2);
        expect(vector.get(0)).toEqual(new Uint8Array([]));
        expect(vector.get(1)).toEqual(new Uint8Array([1]));
    });

    it('should handle exactly 12-byte boundary values', () => {
        const builder = makeBuilder({ type: new BinaryView() });
        const exactly12 = new Uint8Array([1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]);
        const exactly13 = new Uint8Array([1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13]);

        builder.append(exactly12);
        builder.append(exactly13);

        const vector = builder.finish().toVector();
        expect(vector).toHaveLength(2);
        expect(vector.get(0)).toEqual(exactly12);
        expect(vector.get(1)).toEqual(exactly13);
    });

    it('should handle multiple flushes', () => {
        const builder = makeBuilder({ type: new BinaryView() });

        builder.append(new Uint8Array([1, 2]));
        const data1 = builder.flush();
        expect(data1).toHaveLength(1);

        builder.append(new Uint8Array([3, 4]));
        builder.append(new Uint8Array([5, 6]));
        const data2 = builder.flush();
        expect(data2).toHaveLength(2);
    });
});

describe('Utf8ViewBuilder', () => {
    it('should build inline string values (≤12 bytes)', () => {
        const builder = makeBuilder({ type: new Utf8View() });
        const values = ['hello', 'world', 'foo'];

        for (const value of values) {
            builder.append(value);
        }

        const vector = builder.finish().toVector();
        expect(vector).toHaveLength(3);
        expect(vector.get(0)).toBe('hello');
        expect(vector.get(1)).toBe('world');
        expect(vector.get(2)).toBe('foo');
    });

    it('should build out-of-line string values (>12 bytes)', () => {
        const builder = makeBuilder({ type: new Utf8View() });
        const longString = 'This is a long string that exceeds 12 bytes';

        builder.append(longString);

        const vector = builder.finish().toVector();
        expect(vector).toHaveLength(1);
        expect(vector.get(0)).toBe(longString);
    });

    it('should build mixed inline and out-of-line strings', () => {
        const builder = makeBuilder({ type: new Utf8View() });
        const short = 'hi';
        const long = 'This is a very long string that definitely exceeds the 12 byte inline capacity';

        builder.append(short);
        builder.append(long);
        builder.append(short);

        const vector = builder.finish().toVector();
        expect(vector).toHaveLength(3);
        expect(vector.get(0)).toBe(short);
        expect(vector.get(1)).toBe(long);
        expect(vector.get(2)).toBe(short);
    });

    it('should handle null values', () => {
        const builder = makeBuilder({ type: new Utf8View(), nullValues: [null] });

        builder.append('hello');
        builder.append(null);
        builder.append('world');

        const vector = builder.finish().toVector();
        expect(vector).toHaveLength(3);
        expect(vector.get(0)).toBe('hello');
        expect(vector.get(1)).toBeNull();
        expect(vector.get(2)).toBe('world');
    });

    it('should handle empty strings', () => {
        const builder = makeBuilder({ type: new Utf8View() });

        builder.append('');
        builder.append('a');

        const vector = builder.finish().toVector();
        expect(vector).toHaveLength(2);
        expect(vector.get(0)).toBe('');
        expect(vector.get(1)).toBe('a');
    });

    it('should handle UTF-8 multibyte characters', () => {
        const builder = makeBuilder({ type: new Utf8View() });
        const values = ['🚀', '你好', 'Ñoño', 'emoji: 🎉'];

        for (const value of values) {
            builder.append(value);
        }

        const vector = builder.finish().toVector();
        expect(vector).toHaveLength(4);
        expect(vector.get(0)).toBe('🚀');
        expect(vector.get(1)).toBe('你好');
        expect(vector.get(2)).toBe('Ñoño');
        expect(vector.get(3)).toBe('emoji: 🎉');
    });

    it('should handle exactly 12-byte boundary strings', () => {
        const builder = makeBuilder({ type: new Utf8View() });
        const exactly12 = 'twelve bytes'; // ASCII: 12 bytes
        const exactly13 = 'thirteen byte'; // ASCII: 13 bytes

        builder.append(exactly12);
        builder.append(exactly13);

        const vector = builder.finish().toVector();
        expect(vector).toHaveLength(2);
        expect(vector.get(0)).toBe(exactly12);
        expect(vector.get(1)).toBe(exactly13);
    });

    it('should build from vectorFromArray', () => {
        const values = ['hello', 'world', null, 'foo'];
        const vector = vectorFromArray(values, new Utf8View());

        expect(vector).toHaveLength(4);
        expect(vector.get(0)).toBe('hello');
        expect(vector.get(1)).toBe('world');
        expect(vector.get(2)).toBeNull();
        expect(vector.get(3)).toBe('foo');
    });

    it('should handle large batch of values', () => {
        const builder = makeBuilder({ type: new Utf8View() });
        const count = 1000;
        const values: string[] = [];

        for (let i = 0; i < count; i++) {
            const value = i % 2 === 0
                ? `short_${i}` // inline
                : `this_is_a_long_string_that_goes_out_of_line_${i}`; // out-of-line
            values.push(value);
            builder.append(value);
        }

        const vector = builder.finish().toVector();
        expect(vector).toHaveLength(count);

        for (let i = 0; i < count; i++) {
            expect(vector.get(i)).toBe(values[i]);
        }
    });
});
