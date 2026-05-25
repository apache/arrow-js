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

export function arange<T extends { length: number;[n: number]: number }>(arr: T, n = arr.length) {
    for (let i = -1; ++i < n; arr[i] = i) { }
    return arr;
}

/**
 * Declare a `describe` block that runs only when every passed-in symbol is defined,
 * and is otherwise skipped (with a visible reason). Usage:
 *
 *   describeIfExposed('my suite', SymbolA, SymbolB)(() => { ... });
 *
 * Some tests deep-import internal (non-public) exports via `apache-arrow/*`. Those
 * paths resolve on the source and per-module build targets, but single-bundle
 * targets such as the UMD bundle collapse every `apache-arrow/*` import to the
 * public bundle, which doesn't re-export internals — so the bindings are
 * `undefined` there. Gate such suites on the symbols they need so they run
 * everywhere the symbols exist and skip where they don't.
 */
export function describeIfExposed(label: string, ...symbols: unknown[]) {
    const exposed = symbols.every(Boolean);
    if (!exposed) {
        console.warn(
            `Skipping "${label}": this build target (e.g. the UMD bundle) ` +
            `does not expose a required internal export.`
        );
    }
    const run = exposed ? describe : describe.skip;
    return (fn: () => void) => run(label, fn);
}
