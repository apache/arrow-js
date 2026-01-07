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

import { readFileSync } from 'fs';
import { resolve } from 'path';
import { tableFromIPC, RecordBatch } from 'apache-arrow';

// Path to the test file with message-level metadata
// Use process.cwd() since tests are run from project root
const testFilePath = resolve(process.cwd(), 'test/data/test_message_metadata.arrow');

describe('RecordBatch message metadata', () => {
    const buffer = readFileSync(testFilePath);
    const table = tableFromIPC(buffer);

    test('should read RecordBatch metadata from IPC file', () => {
        expect(table.batches.length).toBe(3);

        for (let i = 0; i < table.batches.length; i++) {
            const batch = table.batches[i];
            expect(batch).toBeInstanceOf(RecordBatch);
            expect(batch.metadata).toBeInstanceOf(Map);
            expect(batch.metadata.size).toBeGreaterThan(0);

            // Verify specific metadata keys exist
            expect(batch.metadata.has('batch_index')).toBe(true);
            expect(batch.metadata.has('batch_id')).toBe(true);
            expect(batch.metadata.has('producer')).toBe(true);

            // Verify batch_index matches the batch position
            expect(batch.metadata.get('batch_index')).toBe(String(i));
            expect(batch.metadata.get('batch_id')).toBe(`batch_${String(i).padStart(4, '0')}`);
        }
    });

    test('should read unicode metadata values', () => {
        const batch = table.batches[0];
        expect(batch.metadata.has('unicode_test')).toBe(true);
        expect(batch.metadata.get('unicode_test')).toBe('Hello 世界 🌍 مرحبا');
    });

    test('should handle empty metadata values', () => {
        const batch = table.batches[0];
        expect(batch.metadata.has('optional_field')).toBe(true);
        expect(batch.metadata.get('optional_field')).toBe('');
    });

    test('should read JSON metadata values', () => {
        const batch = table.batches[0];
        expect(batch.metadata.has('batch_info_json')).toBe(true);
        const jsonStr = batch.metadata.get('batch_info_json')!;
        const parsed = JSON.parse(jsonStr);
        expect(parsed.batch_number).toBe(0);
        expect(parsed.processing_stage).toBe('final');
        expect(parsed.tags).toEqual(['validated', 'complete']);
    });

    describe('metadata preservation', () => {
        test('should preserve metadata through slice()', () => {
            const batch = table.batches[0];
            const sliced = batch.slice(0, 2);
            expect(sliced.metadata).toBeInstanceOf(Map);
            expect(sliced.metadata.size).toBe(batch.metadata.size);
            expect(sliced.metadata.get('batch_index')).toBe(batch.metadata.get('batch_index'));
        });

        test('should preserve metadata through select()', () => {
            const batch = table.batches[0];
            const selected = batch.select(['id', 'name']);
            expect(selected.metadata).toBeInstanceOf(Map);
            expect(selected.metadata.size).toBe(batch.metadata.size);
            expect(selected.metadata.get('batch_index')).toBe(batch.metadata.get('batch_index'));
        });

        test('should preserve metadata through selectAt()', () => {
            const batch = table.batches[0];
            const selectedAt = batch.selectAt([0, 1]);
            expect(selectedAt.metadata).toBeInstanceOf(Map);
            expect(selectedAt.metadata.size).toBe(batch.metadata.size);
            expect(selectedAt.metadata.get('batch_index')).toBe(batch.metadata.get('batch_index'));
        });
    });
});
