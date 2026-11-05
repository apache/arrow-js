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

import "../../jest-extensions.js";

import {
    Binary,
    type DataType,
    DenseUnion,
    Field,
    Int32,
    LargeBinary,
    LargeList,
    LargeUtf8,
    List,
    Map_,
    makeData,
    Struct,
    Table,
    tableFromIPC,
    tableToIPC,
    Utf8,
    Vector,
    vectorFromArray,
} from "apache-arrow";

/**
 * The Arrow spec gives the variable-width types exactly `length + 1` offsets,
 * and dense unions exactly `length` offsets.
 *
 * Here we test that the exposed length of offsets to the user is exactly what
 * is defined in the spec.
 *
 * See: https://github.com/apache/arrow-js/issues/484
 */

const ROW_COUNTS = [4, 5, 6];

const mapType = new Map_(
    new Field(
        "entries",
        new Struct<{ key: Utf8; value: Int32 }>([
            new Field("key", new Utf8(), false),
            new Field("value", new Int32(), true),
        ]),
        false,
    ),
);

/** Each case builds `n` rows of a type that carries a valueOffsets buffer. */
const variableWidthCases: [
    name: string,
    type: DataType,
    values: (n: number) => unknown[],
][] = [
        [
            "Utf8",
            new Utf8(),
            (n) => Array.from({ length: n }, (_, i) => "v".repeat(i + 1)),
        ],
        [
            "LargeUtf8",
            new LargeUtf8(),
            (n) => Array.from({ length: n }, (_, i) => "v".repeat(i + 1)),
        ],
        [
            "Binary",
            new Binary(),
            (n) => Array.from({ length: n }, (_, i) => Uint8Array.of(i, i)),
        ],
        [
            "LargeBinary",
            new LargeBinary(),
            (n) => Array.from({ length: n }, (_, i) => Uint8Array.of(i, i)),
        ],
        [
            "List",
            new List(new Field("item", new Int32(), true)),
            (n) => Array.from({ length: n }, (_, i) => [i, i + 1]),
        ],
        [
            "LargeList",
            new LargeList(new Field("item", new Int32(), true)),
            (n) => Array.from({ length: n }, (_, i) => [i, i + 1]),
        ],
        [
            "Map",
            mapType,
            (n) => Array.from({ length: n }, (_, i) => new Map([[`k${i}`, i]])),
        ],
    ];

describe("Data.valueOffsets", () => {
    describe.each(variableWidthCases)("%s", (_name, type, values) => {
        test.each(
            ROW_COUNTS,
        )("has length + 1 offsets straight from the builder (%i rows)", (n) => {
            const data = vectorFromArray(values(n), type).data[0];
            expect(data).toHaveLength(n);
            expect(data.valueOffsets).toHaveLength(n + 1);
        });

        test.each(
            ROW_COUNTS,
        )("has length + 1 offsets after an IPC round trip (%i rows)", (n) => {
            const source = new Table({ col: vectorFromArray(values(n), type) });
            const data = tableFromIPC(tableToIPC(source)).getChild("col")!.data[0];
            expect(data).toHaveLength(n);
            expect(data.valueOffsets).toHaveLength(n + 1);
        });

        test.each(ROW_COUNTS)("round trips its values unchanged (%i rows)", (n) => {
            const source = new Table({ col: vectorFromArray(values(n), type) });
            expect(tableFromIPC(tableToIPC(source))).toEqualTable(source);
        });

        test.each(
            ROW_COUNTS,
        )("keeps length + 1 offsets after slicing (%i rows)", (n) => {
            const data = vectorFromArray(values(n), type).data[0];
            expect(data.slice(1, n - 1).valueOffsets).toHaveLength(n);
        });
    });

    describe("DenseUnion", () => {
        // Dense unions take exactly `length` offsets, with no trailing sentinel.
        const buildUnion = (n: number) => {
            const type = new DenseUnion(
                [0, 1],
                [new Field("a", new Int32(), true), new Field("b", new Utf8(), true)],
            );
            const children = [
                vectorFromArray(
                    Array.from({ length: n }, (_, i) => i),
                    new Int32(),
                ).data[0],
                vectorFromArray(
                    Array.from({ length: n }, (_, i) => `v${i}`),
                    new Utf8(),
                ).data[0],
            ];
            const typeIds = Int8Array.from({ length: n }, (_, i) => i % 2);
            const valueOffsets = Int32Array.from({ length: n }, (_, i) => i >> 1);
            return new Vector([
                makeData({
                    type,
                    length: n,
                    nullCount: 0,
                    typeIds,
                    valueOffsets,
                    children,
                }),
            ]);
        };

        test.each(ROW_COUNTS)("has exactly length offsets (%i rows)", (n) => {
            expect(buildUnion(n).data[0].valueOffsets).toHaveLength(n);
        });

        test.each(
            ROW_COUNTS,
        )("has exactly length offsets after an IPC round trip (%i rows)", (n) => {
            const source = new Table({ col: buildUnion(n) });
            const data = tableFromIPC(tableToIPC(source)).getChild("col")!.data[0];
            expect(data.valueOffsets).toHaveLength(n);
        });

        test.each(
            ROW_COUNTS,
        )("has exactly length offsets after slicing (%i rows)", (n) => {
            // `_sliceBuffers` slices every offsets buffer to `length + 1`, which is one
            // too many for a dense union; the constructor narrows it back down.
            const sliced = buildUnion(n).data[0].slice(1, n - 2);
            expect(sliced.valueOffsets).toHaveLength(sliced.length);
        });
    });
});
