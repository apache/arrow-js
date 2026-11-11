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

import { CompressionType } from '../../fb/compression-type.js';
import { compressionValidators } from './validators.js';

/**
 * A compression codec for IPC record batch body buffers.
 *
 * Arrow JS does not bundle any compression library and does not register any
 * codec by default. To read or write compressed IPC data, register a codec for
 * each `CompressionType` you need with {@link compressionRegistry}.
 *
 * A codec only needs `decode` to read compressed data and only needs `encode`
 * to write it. Both work on a single, complete buffer: `encode` must return the
 * compressed bytes in the format named by the compression type (an LZ4 *frame*
 * for `LZ4_FRAME`, a Zstandard frame for `ZSTD`), and `decode` must return the
 * original uncompressed bytes. Both must be synchronous.
 */
export interface Codec {
    /** Compress one body buffer. Needed to write compressed IPC data. */
    encode?(data: Uint8Array): Uint8Array;
    /** Decompress one body buffer. Needed to read compressed IPC data. */
    decode?(data: Uint8Array): Uint8Array;
}

class _CompressionRegistry {
    protected declare registry: { [key in CompressionType]?: Codec };

    constructor() {
        this.registry = {};
    }

    /**
     * Register (or replace) the codec used for a compression type.
     *
     * If the codec has an `encode` method, it is called once on a small test
     * buffer and the output is checked for the format's magic number, so an
     * error is thrown right away when, for example, a raw LZ4 *block* encoder
     * is registered for `LZ4_FRAME`.
     *
     * @param compression The compression type the codec handles.
     * @param codec The codec to use for that compression type.
     */
    set(compression: CompressionType, codec: Codec) {
        if (codec?.encode && typeof codec.encode === 'function' && !compressionValidators[compression].isValidCodecEncode(codec)) {
            throw new Error(`Encoder for ${CompressionType[compression]} is not valid.`);
        }
        this.registry[compression] = codec;
    }

    /**
     * Get the codec registered for a compression type, or `null` if none is.
     */
    get(compression: CompressionType): Codec | null {
        return this.registry?.[compression] || null;
    }

}

/**
 * The global registry of compression codecs used by the IPC readers and writers.
 *
 * The registry starts out empty. Readers look up a codec only when they meet a
 * compressed record batch or dictionary batch, and throw
 * `Record batch is compressed but codec not found` if no codec with a `decode`
 * method is registered for its compression type. Writers look up a codec only
 * when a `compressionType` is passed (see the writer `compressionType` option
 * and {@link tableToIPC}), and throw if no codec with an `encode` method is
 * registered for it. Uncompressed data never needs a codec, so you only pay
 * for (and load) the compression libraries you register.
 *
 * Codecs are registered process-wide, so register them once at startup,
 * before reading or writing compressed data.
 *
 * @example
 * ```ts
 * import { compressionRegistry, CompressionType, tableFromIPC, tableToIPC } from 'apache-arrow';
 * import * as lz4 from 'lz4js'; // any LZ4 *frame* implementation works
 *
 * compressionRegistry.set(CompressionType.LZ4_FRAME, {
 *     encode: (data) => lz4.compress(data),
 *     decode: (data) => lz4.decompress(data),
 * });
 *
 * const bytes = tableToIPC(table, 'stream', CompressionType.LZ4_FRAME);
 * const roundTripped = tableFromIPC(bytes);
 * ```
 */
export const compressionRegistry = new _CompressionRegistry();
