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
        return super.clear();
    }

    public flush() {
        this._flush();

        // Custom flush logic for ListView
        const { type, length, nullCount, _offsets, _sizes, _nulls } = this;

        const valueOffsets = _offsets.flush(length);
        const sizes = _sizes.flush(length);
        const nullBitmap = nullCount > 0 ? _nulls.flush(length) : undefined;
        const children = this.children.map((child) => child.flush());

        this.clear();

        return makeData({
            type,
            length,
            nullCount,
            nullBitmap,
            valueOffsets,
            sizes,
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

        for (const [index, value] of pending) {
            offsets.reserve(index + 1);
            sizes.reserve(index + 1);

            if (typeof value === 'undefined') {
                // Null or empty list
                offsets.buffer[index] = 0;
                sizes.buffer[index] = 0;
            } else {
                const v = value as T['TValue'];
                const n = v.length;
                const offset = this._writeIndex;

                // Set offset and size directly
                offsets.buffer[index] = offset;
                sizes.buffer[index] = n;

                // Write child values
                for (let i = 0; i < n; i++) {
                    child.set(offset + i, v[i]);
                }

                this._writeIndex += n;
            }
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
        return super.clear();
    }

    public flush() {
        this._flush();

        // Custom flush logic for LargeListView
        const { type, length, nullCount, _offsets, _sizes, _nulls } = this;

        const valueOffsets = _offsets.flush(length);
        const sizes = _sizes.flush(length);
        const nullBitmap = nullCount > 0 ? _nulls.flush(length) : undefined;
        const children = this.children.map((child) => child.flush());

        this.clear();

        return makeData({
            type,
            length,
            nullCount,
            nullBitmap,
            valueOffsets,
            sizes,
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

        for (const [index, value] of pending) {
            offsets.reserve(index + 1);
            sizes.reserve(index + 1);

            if (typeof value === 'undefined') {
                // Null or empty list
                offsets.buffer[index] = BigInt(0);
                sizes.buffer[index] = BigInt(0);
            } else {
                const v = value as T['TValue'];
                const n = v.length;
                const offset = this._writeIndex;

                // Set offset and size directly (using BigInt for LargeListView)
                offsets.buffer[index] = offset;
                sizes.buffer[index] = BigInt(n);

                // Write child values
                for (let i = 0; i < n; i++) {
                    child.set(Number(offset) + i, v[i]);
                }

                this._writeIndex += BigInt(n);
            }
        }
    }
}
