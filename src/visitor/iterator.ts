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

import { Data } from '../data.js';
import { Vector } from '../vector.js';
import { Visitor } from '../visitor.js';
import { Type, Precision } from '../enum.js';
import { TypeToDataType } from '../interfaces.js';
import { instance as getVisitor } from './get.js';
import {
    DataType, Dictionary,
    Bool, Null, Utf8, Utf8View, LargeUtf8, Binary, BinaryView, LargeBinary, Decimal, FixedSizeBinary, List, LargeList, FixedSizeList, Map_, Struct,
    Float, Float16, Float32, Float64,
    Int, Uint8, Uint16, Uint32, Uint64, Int8, Int16, Int32, Int64,
    Date_, DateDay, DateMillisecond,
    Interval, IntervalDayTime, IntervalYearMonth,
    Time, TimeSecond, TimeMillisecond, TimeMicrosecond, TimeNanosecond,
    Timestamp, TimestampSecond, TimestampMillisecond, TimestampMicrosecond, TimestampNanosecond,
    Duration, DurationSecond, DurationMillisecond, DurationMicrosecond, DurationNanosecond,
    Union, DenseUnion, SparseUnion,
    IntervalMonthDayNano,
    RunEndEncoded,
} from '../type.js';
import { ChunkedIterator } from '../util/chunk.js';

/** @ignore */
export interface IteratorVisitor extends Visitor {
    visit<T extends Vector>(node: T): IterableIterator<T['TValue'] | null>;
    visitMany<T extends Vector>(nodes: T[]): IterableIterator<T['TValue'] | null>[];
    getVisitFn<T extends DataType>(node: Vector<T> | T): (vector: Vector<T>) => IterableIterator<T['TValue'] | null>;
    getVisitFn<T extends Type>(node: T): (vector: Vector<TypeToDataType<T>>) => IterableIterator<TypeToDataType<T>['TValue'] | null>;
    visitNull<T extends Null>(vector: Vector<T>): IterableIterator<T['TValue'] | null>;
    visitBool<T extends Bool>(vector: Vector<T>): IterableIterator<T['TValue'] | null>;
    visitInt<T extends Int>(vector: Vector<T>): IterableIterator<T['TValue'] | null>;
    visitInt8<T extends Int8>(vector: Vector<T>): IterableIterator<T['TValue'] | null>;
    visitInt16<T extends Int16>(vector: Vector<T>): IterableIterator<T['TValue'] | null>;
    visitInt32<T extends Int32>(vector: Vector<T>): IterableIterator<T['TValue'] | null>;
    visitInt64<T extends Int64>(vector: Vector<T>): IterableIterator<T['TValue'] | null>;
    visitUint8<T extends Uint8>(vector: Vector<T>): IterableIterator<T['TValue'] | null>;
    visitUint16<T extends Uint16>(vector: Vector<T>): IterableIterator<T['TValue'] | null>;
    visitUint32<T extends Uint32>(vector: Vector<T>): IterableIterator<T['TValue'] | null>;
    visitUint64<T extends Uint64>(vector: Vector<T>): IterableIterator<T['TValue'] | null>;
    visitFloat<T extends Float>(vector: Vector<T>): IterableIterator<T['TValue'] | null>;
    visitFloat16<T extends Float16>(vector: Vector<T>): IterableIterator<T['TValue'] | null>;
    visitFloat32<T extends Float32>(vector: Vector<T>): IterableIterator<T['TValue'] | null>;
    visitFloat64<T extends Float64>(vector: Vector<T>): IterableIterator<T['TValue'] | null>;
    visitUtf8<T extends Utf8>(vector: Vector<T>): IterableIterator<T['TValue'] | null>;
    visitLargeUtf8<T extends LargeUtf8>(vector: Vector<T>): IterableIterator<T['TValue'] | null>;
    visitUtf8View<T extends Utf8View>(vector: Vector<T>): IterableIterator<T['TValue'] | null>;
    visitBinary<T extends Binary>(vector: Vector<T>): IterableIterator<T['TValue'] | null>;
    visitLargeBinary<T extends LargeBinary>(vector: Vector<T>): IterableIterator<T['TValue'] | null>;
    visitBinaryView<T extends BinaryView>(vector: Vector<T>): IterableIterator<T['TValue'] | null>;
    visitFixedSizeBinary<T extends FixedSizeBinary>(vector: Vector<T>): IterableIterator<T['TValue'] | null>;
    visitDate<T extends Date_>(vector: Vector<T>): IterableIterator<T['TValue'] | null>;
    visitDateDay<T extends DateDay>(vector: Vector<T>): IterableIterator<T['TValue'] | null>;
    visitDateMillisecond<T extends DateMillisecond>(vector: Vector<T>): IterableIterator<T['TValue'] | null>;
    visitTimestamp<T extends Timestamp>(vector: Vector<T>): IterableIterator<T['TValue'] | null>;
    visitTimestampSecond<T extends TimestampSecond>(vector: Vector<T>): IterableIterator<T['TValue'] | null>;
    visitTimestampMillisecond<T extends TimestampMillisecond>(vector: Vector<T>): IterableIterator<T['TValue'] | null>;
    visitTimestampMicrosecond<T extends TimestampMicrosecond>(vector: Vector<T>): IterableIterator<T['TValue'] | null>;
    visitTimestampNanosecond<T extends TimestampNanosecond>(vector: Vector<T>): IterableIterator<T['TValue'] | null>;
    visitTime<T extends Time>(vector: Vector<T>): IterableIterator<T['TValue'] | null>;
    visitTimeSecond<T extends TimeSecond>(vector: Vector<T>): IterableIterator<T['TValue'] | null>;
    visitTimeMillisecond<T extends TimeMillisecond>(vector: Vector<T>): IterableIterator<T['TValue'] | null>;
    visitTimeMicrosecond<T extends TimeMicrosecond>(vector: Vector<T>): IterableIterator<T['TValue'] | null>;
    visitTimeNanosecond<T extends TimeNanosecond>(vector: Vector<T>): IterableIterator<T['TValue'] | null>;
    visitDecimal<T extends Decimal>(vector: Vector<T>): IterableIterator<T['TValue'] | null>;
    visitList<T extends List>(vector: Vector<T>): IterableIterator<T['TValue'] | null>;
    visitLargeList<T extends LargeList>(vector: Vector<T>): IterableIterator<T['TValue'] | null>;
    visitStruct<T extends Struct>(vector: Vector<T>): IterableIterator<T['TValue'] | null>;
    visitUnion<T extends Union>(vector: Vector<T>): IterableIterator<T['TValue'] | null>;
    visitDenseUnion<T extends DenseUnion>(vector: Vector<T>): IterableIterator<T['TValue'] | null>;
    visitSparseUnion<T extends SparseUnion>(vector: Vector<T>): IterableIterator<T['TValue'] | null>;
    visitDictionary<T extends Dictionary>(vector: Vector<T>): IterableIterator<T['TValue'] | null>;
    visitInterval<T extends Interval>(vector: Vector<T>): IterableIterator<T['TValue'] | null>;
    visitIntervalDayTime<T extends IntervalDayTime>(vector: Vector<T>): IterableIterator<T['TValue'] | null>;
    visitIntervalYearMonth<T extends IntervalYearMonth>(vector: Vector<T>): IterableIterator<T['TValue'] | null>;
    visitIntervalMonthDayNano<T extends IntervalMonthDayNano>(vector: Vector<T>): IterableIterator<T['TValue'] | null>;
    visitDuration<T extends Duration>(vector: Vector<T>): IterableIterator<T['TValue'] | null>;
    visitDurationSecond<T extends DurationSecond>(vector: Vector<T>): IterableIterator<T['TValue'] | null>;
    visitDurationMillisecond<T extends DurationMillisecond>(vector: Vector<T>): IterableIterator<T['TValue'] | null>;
    visitDurationMicrosecond<T extends DurationMicrosecond>(vector: Vector<T>): IterableIterator<T['TValue'] | null>;
    visitDurationNanosecond<T extends DurationNanosecond>(vector: Vector<T>): IterableIterator<T['TValue'] | null>;
    visitFixedSizeList<T extends FixedSizeList>(vector: Vector<T>): IterableIterator<T['TValue'] | null>;
    visitMap<T extends Map_>(vector: Vector<T>): IterableIterator<T['TValue'] | null>;
    visitRunEndEncoded<T extends RunEndEncoded>(vector: Vector<T>): IterableIterator<T['TValue'] | null>;
}

/** @ignore */
export class IteratorVisitor extends Visitor { }

/** @ignore */
function vectorIterator<T extends DataType>(vector: Vector<T>): IterableIterator<T['TValue'] | null> {

    const { type } = vector;

    // Fast case, defer to native iterators if possible
    if (vector.nullCount === 0 && vector.stride === 1 && (
        // Don't defer to native iterator for timestamps since Numbers are expected
        // (DataType.isTimestamp(type)) && type.unit === TimeUnit.MILLISECOND ||
        (DataType.isInt(type) && type.bitWidth !== 64) ||
        (DataType.isTime(type) && type.bitWidth !== 64) ||
        (DataType.isFloat(type) && type.precision !== Precision.HALF)
    )) {
        return new ChunkedIterator(vector.data.length, (chunkIndex) => {
            const data = vector.data[chunkIndex];
            return data.values.subarray(0, data.length)[Symbol.iterator]();
        });
    }

    // Otherwise, iterate manually
    let offset = 0;
    return new ChunkedIterator(vector.data.length, (chunkIndex) => {
        const data = vector.data[chunkIndex];
        const length = data.length;
        const inner = vector.slice(offset, offset + length);
        offset += length;
        return new VectorIterator(inner);
    });
}

/** @ignore */
function runEndEncodedIterator<T extends RunEndEncoded>(vector: Vector<T>): IterableIterator<T['TValue'] | null> {
    // Use specialized iterator with O(1) amortized sequential access
    let offset = 0;
    return new ChunkedIterator(vector.data.length, (chunkIndex) => {
        const data = vector.data[chunkIndex];
        const length = data.length;
        const inner = vector.slice(offset, offset + length);
        offset += length;
        return new RunEndEncodedIterator(inner);
    });
}

/** @ignore */
class VectorIterator<T extends DataType> implements IterableIterator<T['TValue'] | null> {
    private index = 0;

    constructor(private vector: Vector<T>) { }

    next(): IteratorResult<T['TValue'] | null> {
        if (this.index < this.vector.length) {
            return {
                value: this.vector.get(this.index++)
            };
        }

        return { done: true, value: null };
    }

    [Symbol.iterator]() {
        return this;
    }
}

/** @ignore */
class RunEndEncodedIterator<T extends RunEndEncoded> implements IterableIterator<T['TValue'] | null> {
    private index = 0;
    private lastPhysicalIndex = 0;
    private readonly runEnds: Data<T['runEndsType']>;
    private readonly values: Data<T['valueType']>;
    private readonly getRunEnd: (data: Data<T['runEndsType']>, index: number) => T['runEndsType']['TValue'] | null;
    private readonly getValue: (data: Data<T['valueType']>, index: number) => T['TValue'] | null;

    constructor(private vector: Vector<T>) {
        const data = vector.data[0];
        this.runEnds = data.children[0] as Data<T['runEndsType']>;
        this.values = data.children[1] as Data<T['valueType']>;
        this.getRunEnd = getVisitor.getVisitFn(this.runEnds);
        this.getValue = getVisitor.getVisitFn(this.values);
    }

    next(): IteratorResult<T['TValue'] | null> {
        if (this.index < this.vector.length) {
            const value = this.getValueAtIndex(this.index++);
            return { value };
        }
        return { done: true, value: null };
    }

    private getValueAtIndex(logicalIndex: number): T['TValue'] {
        const physicalIndex = this.findPhysicalIndex(logicalIndex);
        return this.getValue(this.values, physicalIndex);
    }

    private findPhysicalIndex(i: number): number {
        const runEndsLength = this.runEnds.length;
        const offset = this.vector.data[0].offset;

        // Fast path: check if the cached physical index is still valid
        const cachedRunEnd = Number(this.getRunEnd(this.runEnds, this.lastPhysicalIndex));
        if (offset + i < cachedRunEnd) {
            // Cached value is an upper bound, but is it the least upper bound?
            if (this.lastPhysicalIndex === 0) {
                return this.lastPhysicalIndex;
            }
            const prevRunEnd = Number(this.getRunEnd(this.runEnds, this.lastPhysicalIndex - 1));
            if (offset + i >= prevRunEnd) {
                // Cache hit - same run as before
                return this.lastPhysicalIndex;
            }
            // Search in the range before the cached index
            this.lastPhysicalIndex = this.binarySearchRange(0, this.lastPhysicalIndex, i, offset);
            return this.lastPhysicalIndex;
        }

        // Cached index is not an upper bound, search after it
        const minPhysicalIndex = this.lastPhysicalIndex + 1;
        const relativeIndex = this.binarySearchRange(
            minPhysicalIndex,
            runEndsLength,
            i,
            offset
        );
        this.lastPhysicalIndex = relativeIndex;
        return this.lastPhysicalIndex;
    }

    private binarySearchRange(start: number, end: number, i: number, offset: number): number {
        let low = start;
        let high = end - 1;
        while (low < high) {
            const mid = (low + high) >>> 1;
            const runEnd = Number(this.getRunEnd(this.runEnds, mid));
            if (offset + i < runEnd) {
                high = mid;
            } else {
                low = mid + 1;
            }
        }
        return low;
    }

    [Symbol.iterator]() {
        return this;
    }
}

IteratorVisitor.prototype.visitNull = vectorIterator;
IteratorVisitor.prototype.visitBool = vectorIterator;
IteratorVisitor.prototype.visitInt = vectorIterator;
IteratorVisitor.prototype.visitInt8 = vectorIterator;
IteratorVisitor.prototype.visitInt16 = vectorIterator;
IteratorVisitor.prototype.visitInt32 = vectorIterator;
IteratorVisitor.prototype.visitInt64 = vectorIterator;
IteratorVisitor.prototype.visitUint8 = vectorIterator;
IteratorVisitor.prototype.visitUint16 = vectorIterator;
IteratorVisitor.prototype.visitUint32 = vectorIterator;
IteratorVisitor.prototype.visitUint64 = vectorIterator;
IteratorVisitor.prototype.visitFloat = vectorIterator;
IteratorVisitor.prototype.visitFloat16 = vectorIterator;
IteratorVisitor.prototype.visitFloat32 = vectorIterator;
IteratorVisitor.prototype.visitFloat64 = vectorIterator;
IteratorVisitor.prototype.visitUtf8 = vectorIterator;
IteratorVisitor.prototype.visitLargeUtf8 = vectorIterator;
IteratorVisitor.prototype.visitUtf8View = vectorIterator;
IteratorVisitor.prototype.visitBinary = vectorIterator;
IteratorVisitor.prototype.visitLargeBinary = vectorIterator;
IteratorVisitor.prototype.visitBinaryView = vectorIterator;
IteratorVisitor.prototype.visitFixedSizeBinary = vectorIterator;
IteratorVisitor.prototype.visitDate = vectorIterator;
IteratorVisitor.prototype.visitDateDay = vectorIterator;
IteratorVisitor.prototype.visitDateMillisecond = vectorIterator;
IteratorVisitor.prototype.visitTimestamp = vectorIterator;
IteratorVisitor.prototype.visitTimestampSecond = vectorIterator;
IteratorVisitor.prototype.visitTimestampMillisecond = vectorIterator;
IteratorVisitor.prototype.visitTimestampMicrosecond = vectorIterator;
IteratorVisitor.prototype.visitTimestampNanosecond = vectorIterator;
IteratorVisitor.prototype.visitTime = vectorIterator;
IteratorVisitor.prototype.visitTimeSecond = vectorIterator;
IteratorVisitor.prototype.visitTimeMillisecond = vectorIterator;
IteratorVisitor.prototype.visitTimeMicrosecond = vectorIterator;
IteratorVisitor.prototype.visitTimeNanosecond = vectorIterator;
IteratorVisitor.prototype.visitDecimal = vectorIterator;
IteratorVisitor.prototype.visitList = vectorIterator;
IteratorVisitor.prototype.visitLargeList = vectorIterator;
IteratorVisitor.prototype.visitStruct = vectorIterator;
IteratorVisitor.prototype.visitUnion = vectorIterator;
IteratorVisitor.prototype.visitDenseUnion = vectorIterator;
IteratorVisitor.prototype.visitSparseUnion = vectorIterator;
IteratorVisitor.prototype.visitDictionary = vectorIterator;
IteratorVisitor.prototype.visitInterval = vectorIterator;
IteratorVisitor.prototype.visitIntervalDayTime = vectorIterator;
IteratorVisitor.prototype.visitIntervalYearMonth = vectorIterator;
IteratorVisitor.prototype.visitIntervalMonthDayNano = vectorIterator;
IteratorVisitor.prototype.visitDuration = vectorIterator;
IteratorVisitor.prototype.visitDurationSecond = vectorIterator;
IteratorVisitor.prototype.visitDurationMillisecond = vectorIterator;
IteratorVisitor.prototype.visitDurationMicrosecond = vectorIterator;
IteratorVisitor.prototype.visitDurationNanosecond = vectorIterator;
IteratorVisitor.prototype.visitFixedSizeList = vectorIterator;
IteratorVisitor.prototype.visitMap = vectorIterator;
IteratorVisitor.prototype.visitRunEndEncoded = runEndEncodedIterator;

/** @ignore */
export const instance = new IteratorVisitor();
