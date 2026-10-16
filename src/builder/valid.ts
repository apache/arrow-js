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

import { DataType } from '../type.js';

/**
 * Creates a validity predicate without compiling strings as JavaScript.
 * A single null sentinel uses a direct comparison; multiple sentinels use a
 * snapshot Set, including NaN and treating positive and negative zero equally.
 *
 * @example
 * console.log(createIsValidFunction([null, 'N/A', NaN]));
 * `function (x) {
 *     if (x !== x) return false;
 *     switch (x) {
 *         case null:
 *         case "N/A":
 *             return false;
 *     }
 *     return true;
 * }`
 *
 * @ignore
 * @param nullValues
 */
export function createIsValidFunction<T extends DataType = any, TNull = any>(nullValues?: ReadonlyArray<TNull>) {

    if (!nullValues || nullValues.length <= 0) {
        // @ts-ignore
        return function isValid(value: any) { return true; };
    }

    if (nullValues.length === 1) {
        const nullValue = nullValues[0];
        return nullValue !== nullValue
            ? function isValid(value: T['TValue'] | TNull) { return value === value; }
            : function isValid(value: T['TValue'] | TNull) { return value !== nullValue; };
    }
    const invalidValues = new Set<T['TValue'] | TNull>(nullValues);
    return function isValid(value: T['TValue'] | TNull) { return !invalidValues.has(value); };
}
