// Test for issue #90: Strong typing for builders
import * as arrow from './index.mjs';

interface ValueType extends arrow.TypeMap {
    time: arrow.TimestampMillisecond,
    value: arrow.Float64,
}

type Value = {
    time: number,
    value: number,
}

const children: (arrow.Field<arrow.TimestampMillisecond> | arrow.Field<arrow.Float64>)[] = [
    new arrow.Field('time', new arrow.TimestampMillisecond()),
    new arrow.Field('value', new arrow.Float64()),
];

const valueDataType: arrow.Struct<ValueType> = new arrow.Struct<ValueType>(children);
const builder: arrow.StructBuilder<ValueType, null | undefined> = arrow.makeBuilder({
    type: valueDataType,
    nullValues: [null, undefined]
}) as arrow.StructBuilder<ValueType, null | undefined>;

// This should now work without type errors
const add = (value: Value) => builder.append(value);

// Test it
add({ time: Date.now(), value: 42.5 });
add({ time: Date.now(), value: 100.0 });

const vector = builder.finish().toVector();
console.log('Success! Built vector with', vector.length, 'rows');
console.log('First row:', vector.get(0)?.toJSON());
console.log('Second row:', vector.get(1)?.toJSON());
