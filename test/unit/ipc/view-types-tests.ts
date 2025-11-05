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

import { vectorFromArray } from 'apache-arrow';
import { BinaryView, Utf8View } from 'apache-arrow/type';

describe('BinaryView and Utf8View integration', () => {
    const inlineBinary = new Uint8Array([1, 2, 3, 4, 5]);
    const referencedBinary = new Uint8Array(Array.from({ length: 20 }, (_, i) => i));
    const referencedUtf8 = 'View types are fun!';
    const inlineUtf8 = 'hi';

    it('reads BinaryView values via Vector', () => {
        const vector = vectorFromArray([inlineBinary, referencedBinary, null], new BinaryView());
        expect(vector.get(0)).toEqual(inlineBinary);
        expect(vector.get(1)).toEqual(referencedBinary);
        expect(vector.get(2)).toBeNull();
    });

    it('reads Utf8View values via Vector', () => {
        const vector = vectorFromArray([inlineUtf8, referencedUtf8], new Utf8View());
        expect(vector.get(0)).toBe(inlineUtf8);
        expect(vector.get(1)).toBe(referencedUtf8);
    });

});
