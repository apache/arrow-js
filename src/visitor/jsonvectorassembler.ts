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

import { BN } from '../util/bn.js';
import { Data } from '../data.js';
import { Field } from '../schema.js';
import { Vector } from '../vector.js';
import { Visitor } from '../visitor.js';
import { BufferType, IntervalUnit } from '../enum.js';
import { RecordBatch } from '../recordbatch.js';
import { UnionMode, DateUnit, TimeUnit } from '../enum.js';
import { BitIterator, getBit, getBool } from '../util/bit.js';
import { toIntervalDayTimeObjects, toIntervalMonthDayNanoObjects } from '../util/interval.js';
import {
    DataType,
    Float, Int, Date_, Interval, Time, Timestamp, Union, Duration,
    Bool, Null, Utf8, LargeUtf8, Binary, LargeBinary, BinaryView, Utf8View, Decimal, FixedSizeBinary, List, FixedSizeList, Map_, Struct, IntArray,
} from '../type.js';

/** @ignore */
export interface JSONVectorAssembler extends Visitor {

    visit<T extends DataType>(field: Field, node: Data<T>): Record<string, unknown>;
    visitMany<T extends DataType>(fields: Field[], nodes: readonly Data<T>[]): Record<string, unknown>[];
    getVisitFn<T extends DataType>(node: Vector<T> | Data<T>): (data: Data<T>) => { name: string; count: number; VALIDITY: (0 | 1)[]; DATA?: any[]; OFFSET?: number[]; TYPE_ID?: number[]; children?: any[] };

    visitNull<T extends Null>(data: Data<T>): Record<string, never>;
    visitBool<T extends Bool>(data: Data<T>): { DATA: boolean[] };
    visitInt<T extends Int>(data: Data<T>): { DATA: number[] | string[] };
    visitFloat<T extends Float>(data: Data<T>): { DATA: number[] };
    visitUtf8<T extends Utf8>(data: Data<T>): { DATA: string[]; OFFSET: number[] };
    visitLargeUtf8<T extends LargeUtf8>(data: Data<T>): { DATA: string[]; OFFSET: string[] };
    visitBinary<T extends Binary>(data: Data<T>): { DATA: string[]; OFFSET: number[] };
    visitLargeBinary<T extends LargeBinary>(data: Data<T>): { DATA: string[]; OFFSET: string[] };
    visitBinaryView<T extends BinaryView>(data: Data<T>): { VIEWS: any[]; VARIADIC_DATA_BUFFERS: string[] };
    visitUtf8View<T extends Utf8View>(data: Data<T>): { VIEWS: any[]; VARIADIC_DATA_BUFFERS: string[] };
    visitFixedSizeBinary<T extends FixedSizeBinary>(data: Data<T>): { DATA: string[] };
    visitDate<T extends Date_>(data: Data<T>): { DATA: number[] };
    visitTimestamp<T extends Timestamp>(data: Data<T>): { DATA: string[] };
    visitTime<T extends Time>(data: Data<T>): { DATA: number[] };
    visitDecimal<T extends Decimal>(data: Data<T>): { DATA: string[] };
    visitList<T extends List>(data: Data<T>): { children: any[]; OFFSET: number[] };
    visitStruct<T extends Struct>(data: Data<T>): { children: any[] };
    visitUnion<T extends Union>(data: Data<T>): { children: any[]; TYPE_ID: number[] };
    visitInterval<T extends Interval>(data: Data<T>): { DATA: number[] };
    visitDuration<T extends Duration>(data: Data<T>): { DATA: string[] };
    visitFixedSizeList<T extends FixedSizeList>(data: Data<T>): { children: any[] };
    visitMap<T extends Map_>(data: Data<T>): { children: any[] };
}

/** @ignore */
export class JSONVectorAssembler extends Visitor {

    /** @nocollapse */
    public static assemble<T extends RecordBatch>(...batches: T[]) {
        const assembler = new JSONVectorAssembler();
        return batches.map(({ schema, data }) => {
            return assembler.visitMany(schema.fields, data.children);
        });
    }

    public visit<T extends DataType>({ name }: Field, data: Data<T>) {
        const { length } = data;
        const { offset, nullCount, nullBitmap } = data;
        const type = DataType.isDictionary(data.type) ? data.type.indices : data.type;
        const buffers = Object.assign([], data.buffers, { [BufferType.VALIDITY]: undefined });
        return {
            'name': name,
            'count': length,
            'VALIDITY': (DataType.isNull(type) || DataType.isUnion(type))
                ? undefined
                : nullCount <= 0 ? Array.from({ length }, () => 1)
                    : [...new BitIterator(nullBitmap, offset, length, null, getBit)],
            ...super.visit(data.clone(type, offset, length, 0, buffers))
        };
    }
    public visitNull() { return {}; }
    public visitBool<T extends Bool>({ values, offset, length }: Data<T>) {
        return { 'DATA': [...new BitIterator(values, offset, length, null, getBool)] };
    }
    public visitInt<T extends Int>(data: Data<T>) {
        return {
            'DATA': data.type.bitWidth < 64
                ? [...data.values]
                : [...bigNumsToStrings(data.values, 2)]
        };
    }
    public visitFloat<T extends Float>(data: Data<T>) {
        return { 'DATA': [...data.values] };
    }
    public visitUtf8<T extends Utf8>(data: Data<T>) {
        return { 'DATA': [...new Vector([data])], 'OFFSET': [...data.valueOffsets] };
    }
    public visitLargeUtf8<T extends LargeUtf8>(data: Data<T>) {
        return { 'DATA': [...new Vector([data])], 'OFFSET': [...bigNumsToStrings(data.valueOffsets, 2)] };
    }
    public visitBinary<T extends Binary>(data: Data<T>) {
        return { 'DATA': [...binaryToString(new Vector([data]))], 'OFFSET': [...data.valueOffsets] };
    }
    public visitLargeBinary<T extends LargeBinary>(data: Data<T>) {
        return { 'DATA': [...binaryToString(new Vector([data]))], 'OFFSET': [...bigNumsToStrings(data.valueOffsets, 2)] };
    }
    public visitBinaryView<T extends BinaryView>(data: Data<T>) {
        return viewDataToJSON(data, true);
    }
    public visitUtf8View<T extends Utf8View>(data: Data<T>) {
        return viewDataToJSON(data, false);
    }
    public visitFixedSizeBinary<T extends FixedSizeBinary>(data: Data<T>) {
        return { 'DATA': [...binaryToString(new Vector([data]))] };
    }
    public visitDate<T extends Date_>(data: Data<T>) {
        return {
            'DATA': data.type.unit === DateUnit.DAY
                ? [...data.values]
                : [...bigNumsToStrings(data.values, 2)]
        };
    }
    public visitTimestamp<T extends Timestamp>(data: Data<T>) {
        return { 'DATA': [...bigNumsToStrings(data.values, 2)] };
    }
    public visitTime<T extends Time>(data: Data<T>) {
        return {
            'DATA': data.type.unit < TimeUnit.MICROSECOND
                ? [...data.values]
                : [...bigNumsToStrings(data.values, 2)]
        };
    }
    public visitDecimal<T extends Decimal>(data: Data<T>) {
        return { 'DATA': [...bigNumsToStrings(data.values, 4)] };
    }
    public visitList<T extends List>(data: Data<T>) {
        return {
            'OFFSET': [...data.valueOffsets],
            'children': this.visitMany(data.type.children, data.children)
        };
    }
    public visitStruct<T extends Struct>(data: Data<T>) {
        return {
            'children': this.visitMany(data.type.children, data.children)
        };
    }
    public visitUnion<T extends Union>(data: Data<T>) {
        return {
            'TYPE_ID': [...data.typeIds],
            'OFFSET': data.type.mode === UnionMode.Dense ? [...data.valueOffsets] : undefined,
            'children': this.visitMany(data.type.children, data.children)
        };
    }
    public visitInterval<T extends Interval>(data: Data<T>) {
        switch (data.type.unit) {
            case IntervalUnit.YEAR_MONTH:
                return { 'DATA': [...data.values] };
            case IntervalUnit.DAY_TIME:
                return { 'DATA': toIntervalDayTimeObjects(data.values) };
            case IntervalUnit.MONTH_DAY_NANO:
                return { 'DATA': toIntervalMonthDayNanoObjects(data.values, true) };
        }
    }
    public visitDuration<T extends Duration>(data: Data<T>) {
        return { 'DATA': [...bigNumsToStrings(data.values, 2)] };
    }
    public visitFixedSizeList<T extends FixedSizeList>(data: Data<T>) {
        return {
            'children': this.visitMany(data.type.children, data.children)
        };
    }
    public visitMap<T extends Map_>(data: Data<T>) {
        return {
            'OFFSET': [...data.valueOffsets],
            'children': this.visitMany(data.type.children, data.children)
        };
    }
}

/** @ignore */
function* binaryToString(vector: Vector<Binary> | Vector<LargeBinary> | Vector<FixedSizeBinary>) {
    for (const octets of vector as Iterable<Uint8Array>) {
        yield octets.reduce((str, byte) => {
            return `${str}${('0' + (byte & 0xFF).toString(16)).slice(-2)}`;
        }, '').toUpperCase();
    }
}

/** @ignore */
function* bigNumsToStrings(values: BigUint64Array | BigInt64Array | Uint32Array | Int32Array | IntArray, stride: number) {
    const u32s = new Uint32Array(values.buffer);
    for (let i = -1, n = u32s.length / stride; ++i < n;) {
        yield `${BN.new(u32s.subarray((i + 0) * stride, (i + 1) * stride), false)}`;
    }
}

/** @ignore */
function viewDataToJSON(data: Data<BinaryView> | Data<Utf8View>, isBinary: boolean) {
    const INLINE_SIZE = 12;
    const views: any[] = [];
    const variadicBuffers: string[] = [];
    const variadicBuffersMap = new Map<number, number>(); // buffer index in data -> index in output array

    // Read view structs from the views buffer (16 bytes each)
    const viewsData = data.values;
    const dataView = new DataView(viewsData.buffer, viewsData.byteOffset, viewsData.byteLength);
    const numViews = viewsData.byteLength / 16;

    for (let i = 0; i < numViews; i++) {
        const offset = i * 16;
        const size = dataView.getInt32(offset, true);

        if (size <= INLINE_SIZE) {
            // Inline view: read the inlined data (bytes 4-15, up to 12 bytes)
            const inlined = viewsData.subarray(offset + 4, offset + 4 + size);
            const inlinedHex = Array.from(inlined)
                .map(b => ('0' + (b & 0xFF).toString(16)).slice(-2))
                .join('')
                .toUpperCase();

            views.push({
                'SIZE': size,
                'INLINED': isBinary ? inlinedHex : Array.from(inlined).map(b => String.fromCharCode(b)).join('')
            });
        } else {
            // Out-of-line view: read prefix (4 bytes at offset 4-7), buffer_index, offset
            const prefix = viewsData.subarray(offset + 4, offset + 8);
            const prefixHex = Array.from(prefix)
                .map(b => ('0' + (b & 0xFF).toString(16)).slice(-2))
                .join('')
                .toUpperCase();
            const bufferIndex = dataView.getInt32(offset + 8, true);
            const bufferOffset = dataView.getInt32(offset + 12, true);

            // Track which variadic buffers we're using and map to output indices
            if (!variadicBuffersMap.has(bufferIndex)) {
                const outputIndex = variadicBuffers.length;
                variadicBuffersMap.set(bufferIndex, outputIndex);

                // Get the actual buffer data and convert to hex
                const buffer = data.variadicBuffers[bufferIndex];
                const hex = Array.from(buffer)
                    .map(b => ('0' + (b & 0xFF).toString(16)).slice(-2))
                    .join('')
                    .toUpperCase();
                variadicBuffers.push(hex);
            }

            views.push({
                'SIZE': size,
                'PREFIX_HEX': prefixHex,
                'BUFFER_INDEX': variadicBuffersMap.get(bufferIndex),
                'OFFSET': bufferOffset
            });
        }
    }

    return { 'VIEWS': views, 'VARIADIC_DATA_BUFFERS': variadicBuffers };
}
