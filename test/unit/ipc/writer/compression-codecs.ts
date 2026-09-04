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

import { ByteBuffer } from 'flatbuffers';

import {
    Codec,
    compressionRegistry,
    CompressionType,
    MessageReader,
} from 'apache-arrow';
import * as lz4js from 'lz4js';

const LENGTH_NO_COMPRESSED_DATA = -1;
const COMPRESS_LENGTH_PREFIX = 8;
// RecordBatchFileWriter prefixes its output with 6 bytes "ARROW1" + 2 bytes padding
// before the stream messages. Skip past those before handing bytes to MessageReader.
export const FILE_FORMAT_HEADER_LENGTH = 8;

export async function registerCompressionCodecs(): Promise<void> {
    if (compressionRegistry.get(CompressionType.LZ4_FRAME) === null) {
        const lz4Codec: Codec = {
            encode(data: Uint8Array): Uint8Array { return lz4js.compress(data); },
            decode(data: Uint8Array): Uint8Array { return lz4js.decompress(data); }
        };
        compressionRegistry.set(CompressionType.LZ4_FRAME, lz4Codec);
    }

    if (compressionRegistry.get(CompressionType.ZSTD) === null) {
        const { ZstdCodec } = await import('zstd-codec');
        await new Promise<void>((resolve) => {
            ZstdCodec.run((zstd: any) => {
                const simple = new zstd.Simple();
                const zstdCodec: Codec = {
                    encode(data: Uint8Array): Uint8Array { return simple.compress(data); },
                    decode(data: Uint8Array): Uint8Array { return simple.decompress(data); }
                };
                compressionRegistry.set(CompressionType.ZSTD, zstdCodec);
                resolve();
            });
        });
    }
}

/**
 * Walks the IPC messages in `bytes` and returns the (prefix, decompressedLength)
 * pair for every compressed body buffer. Per the Arrow columnar format spec, callers
 * should assert `prefix === decompressedLength` — the eight-byte prefix must hold
 * the uncompressed length so a reader can size the decompression destination buffer.
 */
export function extractCompressedPrefixes(
    bytes: Uint8Array,
    codec: Codec,
): { prefix: number; decompressedLength: number }[] {
    const reader = new MessageReader(bytes);
    const results: { prefix: number; decompressedLength: number }[] = [];

    for (const message of reader) {
        const body = reader.readMessageBody(message.bodyLength);
        if (!message.isRecordBatch()) continue;

        for (const region of message.header().buffers) {
            if (region.length === 0) continue;
            const buffer = body.subarray(region.offset, region.offset + region.length);
            const prefix = Number(new ByteBuffer(buffer).readInt64(0));
            if (prefix === LENGTH_NO_COMPRESSED_DATA) continue;

            const decompressed = codec.decode!(buffer.subarray(COMPRESS_LENGTH_PREFIX));
            results.push({ prefix, decompressedLength: decompressed.length });
        }
    }
    return results;
}
