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
