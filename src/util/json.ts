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

// @ts-ignore
import { parse as bignumJSONParse, BigNumber } from 'json-bignum';

/** @ignore */
export function parseArrowJSON(source: string): any {
    return normalizeUnsafeIntegers(bignumJSONParse(source));
}

function normalizeUnsafeIntegers(value: any): any {
    if (typeof value === 'number') {
        if (Number.isInteger(value) && !Number.isSafeInteger(value)) {
            return new BigNumber(`${value}`);
        }
        return value;
    }
    if (Array.isArray(value)) {
        return value.map((item) => normalizeUnsafeIntegers(item));
    }
    if (value && typeof value === 'object' && !(value instanceof BigNumber)) {
        for (const key of Object.keys(value)) {
            value[key] = normalizeUnsafeIntegers(value[key]);
        }
    }
    return value;
}
