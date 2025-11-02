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

import { Utf8View, BinaryView } from '../type.js';
import { encodeUtf8 } from '../util/utf8.js';
import { BuilderOptions, Builder } from '../builder.js';
import { BufferBuilder } from './buffer.js';
import { makeData } from '../data.js';

/** @ignore */
export class Utf8ViewBuilder<TNull = any> extends Builder<Utf8View, TNull> {
    protected _views: BufferBuilder<Uint8Array>;
    protected _variadicBuffers: Uint8Array[] = [];
    protected _currentBuffer: BufferBuilder<Uint8Array> | null = null;
    protected _currentBufferIndex = 0;
    protected _currentBufferOffset = 0;
    protected readonly _bufferSize = 32 * 1024 * 1024;

    constructor(opts: BuilderOptions<Utf8View, TNull>) {
        super(opts);
        this._views = new BufferBuilder(Uint8Array);
    }

    public get byteLength(): number {
        let size = 0;
        this._views && (size += this._views.byteLength);
        this._nulls && (size += this._nulls.byteLength);
        size += this._variadicBuffers.reduce((acc, buffer) => acc + buffer.byteLength, 0);
        this._currentBuffer && (size += this._currentBuffer.byteLength);
        return size;
    }

    public setValue(index: number, value: string) {
        const data = encodeUtf8(value);
        const length = data.length;

        // Ensure views buffer has space up to this index
        const bytesNeeded = (index + 1) * BinaryView.ELEMENT_WIDTH;
        const currentBytes = this._views.length;
        if (bytesNeeded > currentBytes) {
            this._views.reserve(bytesNeeded - currentBytes);
        }

        const viewBuffer = this._views.buffer;
        const viewOffset = index * BinaryView.ELEMENT_WIDTH;
        const view = new DataView(viewBuffer.buffer, viewBuffer.byteOffset + viewOffset, BinaryView.ELEMENT_WIDTH);

        view.setInt32(BinaryView.LENGTH_OFFSET, length, true);

        if (length <= BinaryView.INLINE_CAPACITY) {
            viewBuffer.set(data, viewOffset + BinaryView.INLINE_OFFSET);
            for (let i = length; i < BinaryView.INLINE_CAPACITY; i++) {
                viewBuffer[viewOffset + BinaryView.INLINE_OFFSET + i] = 0;
            }
        } else {
            const prefix = new DataView(data.buffer, data.byteOffset, Math.min(4, length));
            view.setUint32(BinaryView.INLINE_OFFSET, prefix.getUint32(0, true), true);

            if (!this._currentBuffer || this._currentBufferOffset + length > this._bufferSize) {
                if (this._currentBuffer) {
                    this._variadicBuffers.push(this._currentBuffer.buffer.slice(0, this._currentBufferOffset));
                }
                this._currentBuffer = new BufferBuilder(Uint8Array);
                this._currentBufferIndex = this._variadicBuffers.length;
                this._currentBufferOffset = 0;
            }

            const bufferData = this._currentBuffer.reserve(length).buffer;
            bufferData.set(data, this._currentBufferOffset);

            view.setInt32(BinaryView.BUFFER_INDEX_OFFSET, this._currentBufferIndex, true);
            view.setInt32(BinaryView.BUFFER_OFFSET_OFFSET, this._currentBufferOffset, true);

            this._currentBufferOffset += length;
        }

        return this;
    }

    public setValid(index: number, isValid: boolean) {
        if (!super.setValid(index, isValid)) {
            // Ensure space is allocated
            const bytesNeeded = (index + 1) * BinaryView.ELEMENT_WIDTH;
            const currentBytes = this._views.length;
            if (bytesNeeded > currentBytes) {
                this._views.reserve(bytesNeeded - currentBytes);
            }

            const viewBuffer = this._views.buffer;
            const viewOffset = index * BinaryView.ELEMENT_WIDTH;
            for (let i = 0; i < BinaryView.ELEMENT_WIDTH; i++) {
                viewBuffer[viewOffset + i] = 0;
            }
            return false;
        }
        return true;
    }

    public clear() {
        this._variadicBuffers = [];
        this._currentBuffer = null;
        this._currentBufferIndex = 0;
        this._currentBufferOffset = 0;
        this._views.clear();
        return super.clear();
    }

    public flush() {
        const { type, length, nullCount, _views, _nulls } = this;

        if (this._currentBuffer && this._currentBufferOffset > 0) {
            this._variadicBuffers.push(this._currentBuffer.buffer.slice(0, this._currentBufferOffset));
            this._currentBuffer = null;
            this._currentBufferOffset = 0;
        }

        const views = _views.flush(length * BinaryView.ELEMENT_WIDTH);
        const nullBitmap = nullCount > 0 ? _nulls.flush(length) : undefined;
        const variadicBuffers = this._variadicBuffers.slice();

        this._variadicBuffers = [];
        this._currentBufferIndex = 0;

        this.clear();

        return makeData({
            type,
            length,
            nullCount,
            nullBitmap,
            views,
            variadicBuffers
        });
    }

    public finish() {
        this.finished = true;
        return this;
    }
}
