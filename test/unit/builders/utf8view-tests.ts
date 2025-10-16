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

import 'web-streams-polyfill';

import {Utf8View, Utf8ViewBuilder} from 'apache-arrow';
import {decodeUtf8} from "../../../src/util/utf8.js";
import {extractViewForLongElement, extractViewForShortElement} from "../../../src/util/buffer.js";

describe('Utf8ViewBuilder', () => {
    it('should represent bytes in array correctly', async () => {
        //todo refactor
        const builder = new Utf8ViewBuilder({type: new Utf8View()});
        const shortStr1 = 'lessthan12';
        const longStr1 = 'thisisverylongstring';
        const shortStr2 = 'short';
        const longStr2 = 'anotherverylongstringgg';
        // const longStr3 = 'cacscasacscscacsacsaca';
        builder.append(shortStr1);
        builder.append(longStr1);
        builder.append(shortStr2);
        builder.append(longStr2);
        // builder.append(longStr3);

        const utf8View = builder.flush();
        if (!utf8View) throw new Error('utf8View is null');

        const view = utf8View.views
        const data = utf8View.values

        // Short string 1

        let shortElement = extractViewForShortElement(view.slice(0, 16));
        expect(shortElement.strLength).toBe(shortStr1.length);
        expect(shortElement.str).toBe(shortStr1);

        // Long string 1
        let longElement = extractViewForLongElement(view.slice(16, 32));
        expect(longElement.strLength).toBe(longStr1.length);
        expect(longElement.strPrefix).toBe(longStr1.slice(0, 4));
        expect(longElement.index).toBe(0);
        expect(longElement.offset).toBe(0);

        // Short string 2
        shortElement = extractViewForShortElement(view.slice(32, 48));
        expect(shortElement.strLength).toBe(shortStr2.length);
        expect(shortElement.str).toBe(shortStr2);

        // Long string 2
        longElement = extractViewForLongElement(view.slice(48));
        expect(longElement.strLength).toBe(longStr2.length);
        expect(longElement.strPrefix).toBe(longStr2.slice(0, 4));
        expect(longElement.index).toBe(0);
        expect(longElement.offset).toBe(20);

        // Check datas array
        const buffer = data
        expect(buffer).toBeDefined();
        expect(decodeUtf8(buffer.slice(0, longStr1.length), true)).toEqual(longStr1);
        expect(decodeUtf8(buffer.slice(longStr1.length, longStr1.length + longStr2.length), true)).toEqual(longStr2);

    })

    it('setValue() replace correct elements', async () => {
        //todo refactor this to use variables
        const builder = new Utf8ViewBuilder({type: new Utf8View()});
        builder.append('string1');
        builder.append('string2');
        builder.append('string3');
        builder.setValue(1, 'newstring2');
        builder.setValue(2, 'veryverylongstring');

        const utf8view = builder.flush();
        if (!utf8view) throw new Error('utf8View is null');

        let uint8Array = utf8view.views.slice(Utf8View.LENGTH_WIDTH, 16);
        expect(decodeUtf8(uint8Array, true)).toBe('string1');
        expect(decodeUtf8(uint8Array, true)).toBe('string1');

        uint8Array = utf8view.views.slice(Utf8View.ELEMENT_WIDTH + Utf8View.LENGTH_WIDTH, 32);
        expect(decodeUtf8(uint8Array, true)).toBe('newstring2');
        expect(decodeUtf8(uint8Array, true)).toBe('newstring2');

        uint8Array = utf8view.views.slice(Utf8View.ELEMENT_WIDTH * 2);
        const longElement = extractViewForLongElement(uint8Array);
        expect(longElement.strPrefix).toBe('very');
        const value = utf8view.values.slice(longElement.offset, longElement.offset + longElement.strLength);
        expect(decodeUtf8(value, true)).toBe('veryverylongstring');
    })

    it('reusing the builder', async () => {
        const shortStr = 'string1';
        const longStr1 = 'veryverylongstring';
        let builder = new Utf8ViewBuilder({type: new Utf8View()});
        builder.append(shortStr);
        builder.append(longStr1);

        builder = builder.finish();
        let buffer = builder.views?.subarray(0, Utf8View.ELEMENT_WIDTH) ?? new Uint8Array(0);
        const shortElement = extractViewForShortElement(buffer);
        expect(shortElement.str).toEqual(shortStr);

        buffer = builder.views?.subarray(Utf8View.ELEMENT_WIDTH, 32) ?? new Uint8Array(0);
        let longElement = extractViewForLongElement(buffer);
        expect(longElement.strPrefix).toBe(longStr1.slice(0, 4));
        let str = decodeUtf8(builder.values?.buffer.slice(0, longStr1.length), true);
        expect(str).toEqual(longStr1);

        const longStr2 = 'This is a new string'
        builder.append(longStr2);
        const utf8view = builder.flush();

        const viewBuffer = utf8view.views?.slice(Utf8View.ELEMENT_WIDTH * 2, Utf8View.ELEMENT_WIDTH * 2 + Utf8View.ELEMENT_WIDTH);
        longElement = extractViewForLongElement(viewBuffer);
        expect(longElement.strPrefix).toBe(longStr2.slice(0, 4));
        str = decodeUtf8(utf8view.values.buffer.slice(longStr1.length, longStr1.length + longStr2.length), true);
        expect(str).toBe(longStr2);

    })

    it('call clear() remove all data in the builder', async () => {
        let builder = new Utf8ViewBuilder({type: new Utf8View()});
        builder.append('string1');
        builder.append('verylongstring');
        builder = builder.finish();
        builder.append('some string');
        builder.clear();
        expect(builder.finished).toBeTruthy();
        expect(builder.values?.length).toBe(0);
        expect(builder.views?.length).toBe(0);
        expect(builder).toHaveLength(0);
    })
});
