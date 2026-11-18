// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
// http://www.apache.org/licenses/LICENSE-2.0

import { Dictionary, Int32, Utf8 } from 'apache-arrow';
import { VectorLoader } from '../../../src/visitor/vectorloader.js';

describe('VectorLoader', () => {
    test('throws when a dictionary batch was not sent', () => {
        const type = new Dictionary(new Utf8(), new Int32(), 42);
        const loader = new VectorLoader(
            new Uint8Array(4),
            [{ length: 1, nullCount: 0 }],
            [{ offset: 0, length: 0 }, { offset: 0, length: 4 }],
            new Map()
        );

        expect(() => loader.visit(type)).toThrow(
            'dictionary id 42 was not sent'
        );
    });
});
