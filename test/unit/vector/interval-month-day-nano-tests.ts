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

import { IntervalMonthDayNano, IntervalMonthDayNanoObject, Vector, makeData, util } from 'apache-arrow';
import { parseArrowJSON } from '../../../src/util/json.js';

const { toIntervalMonthDayNanoInt32Array, toIntervalMonthDayNanoObjects } = util;

function makeIntervalMonthDayNanoVector(array: Int32Array): Vector {
    const type = new IntervalMonthDayNano();
    const length = array.length;
    return new Vector([makeData({ type, length, data: array })]);
}

const EMPTY_INTERVAL_MONTH_DAY_NANO_OBJECT: Partial<IntervalMonthDayNanoObject> = {
    months: 0,
    days: 0,
    nanoseconds: BigInt(0),
};

describe(`MonthDayNanoIntervalVector`, () => {
    test(`Intervals with months are stored in IntervalMonthDayNano`, () => {
        const obj: Partial<IntervalMonthDayNanoObject> = { months: 5 };
        const array = toIntervalMonthDayNanoInt32Array([obj]);
        const vec = makeIntervalMonthDayNanoVector(array);
        expect(vec.type).toBeInstanceOf(IntervalMonthDayNano);
        expect(vec.get(0)).toStrictEqual(array);
        expect(toIntervalMonthDayNanoObjects(vec.get(0), false)).toStrictEqual([{ ...EMPTY_INTERVAL_MONTH_DAY_NANO_OBJECT, ...obj }]);
    });

    test(`Intervals with days are stored in IntervalMonthDayNano`, () => {
        const obj: Partial<IntervalMonthDayNanoObject> = { days: 1000 };
        const array = toIntervalMonthDayNanoInt32Array([obj]);
        const vec = makeIntervalMonthDayNanoVector(array);
        expect(vec.type).toBeInstanceOf(IntervalMonthDayNano);
        expect(vec.get(0)).toStrictEqual(array);
        expect(toIntervalMonthDayNanoObjects(vec.get(0), false)).toStrictEqual([{ ...EMPTY_INTERVAL_MONTH_DAY_NANO_OBJECT, ...obj }]);
    });

    test(`Intervals with nanoseconds are stored in IntervalMonthDayNano`, () => {
        const obj: Partial<IntervalMonthDayNanoObject> = { nanoseconds: 100000000000000000n };
        const array = toIntervalMonthDayNanoInt32Array([obj]);
        const vec = makeIntervalMonthDayNanoVector(array);
        expect(vec.type).toBeInstanceOf(IntervalMonthDayNano);
        expect(vec.get(0)).toStrictEqual(array);
        expect(toIntervalMonthDayNanoObjects(vec.get(0), false)).toStrictEqual([{ ...EMPTY_INTERVAL_MONTH_DAY_NANO_OBJECT, ...obj }]);
    });

    test(`Intervals with months, days, nanoseconds are stored in IntervalMonthDayNano`, () => {
        const obj: Partial<IntervalMonthDayNanoObject> = { months: 1000, days: 10000, nanoseconds: 100000000000000000n };
        const array = toIntervalMonthDayNanoInt32Array([obj]);
        const vec = makeIntervalMonthDayNanoVector(array);
        expect(vec.type).toBeInstanceOf(IntervalMonthDayNano);
        expect(vec.get(0)).toStrictEqual(array);
        expect(toIntervalMonthDayNanoObjects(vec.get(0), false)).toStrictEqual([{ ...EMPTY_INTERVAL_MONTH_DAY_NANO_OBJECT, ...obj }]);

    });

    test(`Negative Intervals with months, days, nanoseconds are stored in IntervalMonthDayNano`, () => {
        const obj: Partial<IntervalMonthDayNanoObject> = { months: -1000, days: -10000, nanoseconds: -100000000000000000n };
        const array = toIntervalMonthDayNanoInt32Array([obj]);
        const vec = makeIntervalMonthDayNanoVector(array);
        expect(vec.type).toBeInstanceOf(IntervalMonthDayNano);
        expect(vec.get(0)).toStrictEqual(array);
        expect(toIntervalMonthDayNanoObjects(vec.get(0), false)).toStrictEqual([{ ...EMPTY_INTERVAL_MONTH_DAY_NANO_OBJECT, ...obj }]);
    });

    test(`Unsafe integer nanoseconds represented as bigint roundtrip correctly`, () => {
        const samples = [
            '-390122861233460600',
            '6684525287992311000'
        ];
        for (const sample of samples) {
            const nanoseconds = BigInt(sample);
            const obj: Partial<IntervalMonthDayNanoObject> = { nanoseconds };
            const array = toIntervalMonthDayNanoInt32Array([obj]);
            const vec = makeIntervalMonthDayNanoVector(array);
            expect(vec.type).toBeInstanceOf(IntervalMonthDayNano);
            expect(vec.get(0)).toStrictEqual(array);
            expect(toIntervalMonthDayNanoObjects(vec.get(0), false)).toStrictEqual([{
                ...EMPTY_INTERVAL_MONTH_DAY_NANO_OBJECT,
                nanoseconds,
            }]);
        }
    });

    test(`Unsafe integer nanoseconds parsed from JSON preserve exact values`, () => {
        const samples = [
            '6684525287992311000',
            '-390122861233460600'
        ];
        for (const sample of samples) {
            const parsed = parseArrowJSON(`{"nanoseconds":${sample}}`);
            expect(typeof parsed.nanoseconds).toBe('bigint');
            const array = toIntervalMonthDayNanoInt32Array([parsed]);
            const vec = makeIntervalMonthDayNanoVector(array);
            expect(vec.type).toBeInstanceOf(IntervalMonthDayNano);
            expect(vec.get(0)).toStrictEqual(array);
            expect(toIntervalMonthDayNanoObjects(array, false)).toStrictEqual([{
                ...EMPTY_INTERVAL_MONTH_DAY_NANO_OBJECT,
                nanoseconds: BigInt(sample)
            }]);
        }
    });
});
