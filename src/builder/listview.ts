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

import { Field } from '../schema.js';
import { Vector } from '../vector.js';
import { DataType, ListView, LargeListView } from '../type.js';
import { DataBufferBuilder } from './buffer.js';
import { Builder, BuilderOptions } from '../builder.js';
import { makeData } from '../data.js';

/** @ignore */
export class ListViewBuilder<T extends DataType = any, TNull = any> extends Builder<ListView<T>, TNull> {
    protected _offsets: DataBufferBuilder<Int32Array>;
    protected _sizes: DataBufferBuilder<Int32Array>;
    protected _pending: Map<number, T['TValue'] | undefined> | undefined;
    protected _writeIndex = 0;

    constructor(opts: BuilderOptions<ListView<T>, TNull>) {
        super(opts);
        this._offsets = new DataBufferBuilder(Int32Array, 0);
        this._sizes = new DataBufferBuilder(Int32Array, 0);
    }

    public addChild(child: Builder<T>, name = '0') {
        if (this.numChildren > 0) {
            throw new Error('ListViewBuilder can only have one child.');
        }
        this.children[this.numChildren] = child;
        this.type = new ListView(new Field(name, child.type, true));
        return this.numChildren - 1;
    }

    public setValue(index: number, value: T['TValue']) {
        const pending = this._pending || (this._pending = new Map());
        pending.set(index, value);
    }

    public setValid(index: number, isValid: boolean) {
        if (!super.setValid(index, isValid)) {
            (this._pending || (this._pending = new Map())).set(index, undefined);
            return false;
        }
        return true;
    }

    public clear() {
        this._pending = undefined;
        this._writeIndex = 0;
        this._offsets.clear();
        this._sizes.clear();
        return super.clear();
    }

    public flush() {
        this._flush();

        // Custom flush logic for ListView
        const type = this.type;
        const length = this.length;
        const nullCount = this.nullCount;
        const offsetsBuilder = this._offsets;
        const sizesBuilder = this._sizes;
        const nullsBuilder = this._nulls;

        const valueOffsets = offsetsBuilder.flush(length);
        const valueSizes = sizesBuilder.flush(length);
        const nullBitmap = nullCount > 0 ? nullsBuilder.flush(length) : undefined;
        const children = this.children.map((child) => child.flush());

        this.clear();

        return makeData({
            type,
            length,
            nullCount,
            nullBitmap,
            valueOffsets,
            valueSizes,
            child: children[0]
        });
    }

    public finish() {
        this._flush();
        return super.finish();
    }

    protected _flush() {
        const pending = this._pending;
        this._pending = undefined;
        if (pending && pending.size > 0) {
            this._flushPending(pending);
        }
    }

    protected _flushPending(pending: Map<number, T['TValue'] | undefined>) {
        const offsets = this._offsets;
        const sizes = this._sizes;
        const [child] = this.children;

        const entries = [...pending.entries()].sort((a, b) => a[0] - b[0]);
        for (const [index, value] of entries) {
            const offset = this._writeIndex;
            offsets.set(index, offset);

            if (typeof value === 'undefined') {
                sizes.set(index, 0);
                continue;
            }

            const listValues = value as T['TValue'];
            const length = Array.isArray(listValues)
                ? listValues.length
                : (listValues as Vector).length;
            sizes.set(index, length);

            for (let i = 0; i < length; i++) {
                const element = Array.isArray(listValues)
                    ? listValues[i]
                    : (listValues as Vector).get(i);
                if (element == null) {
                    child.setValid(offset + i, false);
                } else {
                    child.set(offset + i, element as any);
                }
            }

            this._writeIndex += length;
        }
    }
}

/** @ignore */
export class LargeListViewBuilder<T extends DataType = any, TNull = any> extends Builder<LargeListView<T>, TNull> {
    protected _offsets: DataBufferBuilder<BigInt64Array>;
    protected _sizes: DataBufferBuilder<BigInt64Array>;
    protected _pending: Map<number, T['TValue'] | undefined> | undefined;
    protected _writeIndex = BigInt(0); // BigInt for LargeListView

    constructor(opts: BuilderOptions<LargeListView<T>, TNull>) {
        super(opts);
        this._offsets = new DataBufferBuilder(BigInt64Array, 0);
        this._sizes = new DataBufferBuilder(BigInt64Array, 0);
    }

    public addChild(child: Builder<T>, name = '0') {
        if (this.numChildren > 0) {
            throw new Error('LargeListViewBuilder can only have one child.');
        }
        this.children[this.numChildren] = child;
        this.type = new LargeListView(new Field(name, child.type, true));
        return this.numChildren - 1;
    }

    public setValue(index: number, value: T['TValue']) {
        const pending = this._pending || (this._pending = new Map());
        pending.set(index, value);
    }

    public setValid(index: number, isValid: boolean) {
        if (!super.setValid(index, isValid)) {
            (this._pending || (this._pending = new Map())).set(index, undefined);
            return false;
        }
        return true;
    }

    public clear() {
        this._pending = undefined;
        this._writeIndex = BigInt(0);
        this._offsets.clear();
        this._sizes.clear();
        return super.clear();
    }

    public flush() {
        this._flush();

        // Custom flush logic for LargeListView
        const type = this.type;
        const length = this.length;
        const nullCount = this.nullCount;
        const offsetsBuilder = this._offsets;
        const sizesBuilder = this._sizes;
        const nullsBuilder = this._nulls;

        const valueOffsets = offsetsBuilder.flush(length);
        const valueSizes = sizesBuilder.flush(length);
        const nullBitmap = nullCount > 0 ? nullsBuilder.flush(length) : undefined;
        const children = this.children.map((child) => child.flush());

        this.clear();

        return makeData({
            type,
            length,
            nullCount,
            nullBitmap,
            valueOffsets,
            valueSizes,
            child: children[0]
        });
    }

    public finish() {
        this._flush();
        return super.finish();
    }

    protected _flush() {
        const pending = this._pending;
        this._pending = undefined;
        if (pending && pending.size > 0) {
            this._flushPending(pending);
        }
    }

    protected _flushPending(pending: Map<number, T['TValue'] | undefined>) {
        const offsets = this._offsets;
        const sizes = this._sizes;
        const [child] = this.children;

        const entries = [...pending.entries()].sort((a, b) => a[0] - b[0]);
        for (const [index, value] of entries) {
            const offset = this._writeIndex;
            offsets.set(index, offset);

            if (typeof value === 'undefined') {
                sizes.set(index, BigInt(0));
                continue;
            }

            const listValues = value as T['TValue'];
            const numericLength = Array.isArray(listValues)
                ? listValues.length
                : (listValues as Vector).length;
            const length = BigInt(numericLength);
            sizes.set(index, length);

            for (let i = 0; i < numericLength; i++) {
                const element = Array.isArray(listValues)
                    ? listValues[i]
                    : (listValues as Vector).get(i);
                const targetIndex = Number(offset + BigInt(i));
                if (element == null) {
                    child.setValid(targetIndex, false);
                } else {
                    child.set(targetIndex, element as any);
                }
            }

            this._writeIndex += length;
        }
    }
}
