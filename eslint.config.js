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

import eslint from "@eslint/js";
import tseslint from "typescript-eslint";
import eslintPluginUnicorn from "eslint-plugin-unicorn";
import eslintPluginJest from "eslint-plugin-jest";

export default tseslint.config(
    {
        ignores: [
            "src/fb/**/*",
            "test/bundle/**/*.js",
        ],
    },
    eslint.configs.recommended,
    tseslint.configs.recommended,
    eslintPluginUnicorn.configs["flat/recommended"],
    eslintPluginJest.configs["flat/recommended"],
    eslintPluginJest.configs["flat/style"],
    {
        languageOptions: {
            parserOptions: {
                project: [
                    "tsconfig.json",
                    "tsconfig/tsconfig.bin.cjs.json",
                ],
            },
        },
        rules: {
            "@typescript-eslint/ban-ts-comment": "off",
            "@typescript-eslint/no-explicit-any": "off",
            "@typescript-eslint/no-misused-new": "off",
            "@typescript-eslint/no-namespace": ["error", {
                "allowDeclarations": true,
            }],
            "@typescript-eslint/no-unsafe-declaration-merging": "off",
            "@typescript-eslint/no-unused-expressions": "off",
            "@typescript-eslint/no-unused-vars": "off",
            "no-cond-assign": "off",
            "no-empty": "off",
            "prefer-const": ["error", {
                "destructuring": "all",
            }],
            "unicorn/catch-error-name": "off",
            "unicorn/consistent-function-scoping": "warn",
            "unicorn/empty-brace-spaces": "off",
            "unicorn/filename-case": "off",
            "unicorn/no-array-reduce": "off",
            "unicorn/no-await-expression-member": "off",
            "unicorn/no-negated-condition": "off",
            "unicorn/no-nested-ternary": "off",
            "unicorn/no-new-array": "off",
            "unicorn/no-null": "off",
            "unicorn/no-typeof-undefined": "off",
            "unicorn/no-zero-fractions": "off",
            "unicorn/numeric-separators-style": "off",
            "unicorn/prefer-export-from": "off",
            "unicorn/prefer-math-min-max": "off",
            "unicorn/prefer-spread": "off",
            "unicorn/prefer-switch": "off",
            "unicorn/prefer-top-level-await": "off",
            "unicorn/prevent-abbreviations": "off",
            "unicorn/switch-case-braces": "off",
            "unicorn/text-encoding-identifier-case": "off",
        },
    },
    {
        files: [
            "test/**",
        ],
        rules: {
            "jest/expect-expect": "off",
            "jest/no-conditional-expect": "off",
            "jest/no-export": "off",
            "jest/valid-title": "off",
            "unicorn/consistent-function-scoping": "off",
            "unicorn/no-useless-spread": "off",
        },
    }
);