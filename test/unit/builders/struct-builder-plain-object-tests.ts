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

import { makeBuilder, Field, Struct, StructBuilder, TimestampMillisecond, Float64 } from '../../../src/Arrow.js';

interface ValueType {
  time: TimestampMillisecond;
  value: Float64;
}

type Value = {
  time: number;
  value: number;
};

test('StructBuilder accepts plain object and produces correct values', () => {
  const children = [
    new Field('time', new TimestampMillisecond()),
    new Field('value', new Float64()),
  ];
  const structType = new Struct<any>(children as any);
  const builder = makeBuilder({ type: structType, nullValues: [null, undefined] }) as StructBuilder<any, null | undefined>;

  const value: Value = { time: 1_630_000_000_000, value: 42.5 };
  builder.append(value);
  const vec = builder.finish().toVector();
  const row = vec.get(0)! as any;

  expect(row.time).toBe(value.time);
  expect(row.value).toBeCloseTo(value.value);
  expect(row.toJSON()).toEqual({ time: value.time, value: value.value });
});
