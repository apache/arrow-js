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

import { Table } from "./table.js"

const jupyterDisplay = Symbol.for("Jupyter.display");

const rawMap = {
    '&': '&amp;',
    '<': '&lt;',
    '>': '&gt;',
    '"': '&quot;',
    "'": '&#39;'
} as const;

/**
 * Escapes text for safe interpolation into HTML.
 */
function escapeHTML(str: string) {
    return str.replace(/[&<>"']/g, (char) => rawMap[char as keyof typeof rawMap]);
}

/**
 * Render the table as HTML table element.
 */
function tableToHTML(table: Table): string {
    let htmlTable = `<table class="dataframe">`;

    // Add table headers
    htmlTable += "<thead><tr>";
    for (const field of table.schema.fields) {
      htmlTable += `<th>${escapeHTML(field.name)}</th>`;
    }
    htmlTable += "</tr></thead>";
    // Add table data
    htmlTable += "<tbody>";
    for (const row of table) {
        htmlTable += "<tr>";
        for (const field of row.toArray()) {
            htmlTable += `<td>${escapeHTML(String(field))}</td>`;
        }
        htmlTable += "</tr>";
    }
    htmlTable += "</tbody></table>";

    return htmlTable;
}

declare module "./table.js" {
    interface Table {
        [jupyterDisplay](): { "text/html": string };
    }
}

Table.prototype[jupyterDisplay] = function(this: Table) {
    // TODO: from env or options
    const rows = 50
    const limited = this.slice(0, rows);
    return {
        // TODO: application/vnd.dataresource+json
        "text/html": tableToHTML(limited)
    }
}
