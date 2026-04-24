/**
 * Process raw arrays of data into structured key-value rows
 */
const processRawData = (rawData, headerIndex) => {
    if (!rawData || !rawData.rows || !rawData.rows.length) return { headers: [], rows: [] };
    
    const headers = rawData.rows[headerIndex] || [];
    // Ensure unique headers to prevent collisions
    const uniqueHeaders = headers.map((h, i) => h !== null && h !== undefined ? String(h).trim() : `Column_${i}`);
    
    const dataRows = rawData.rows.slice(headerIndex + 1).map(row => {
        const obj = {};
        uniqueHeaders.forEach((header, index) => {
            obj[header] = row[index];
        });
        return obj;
    });

    return { headers: uniqueHeaders, rows: dataRows };
};

/**
 * Perform joins and appends for multiple tables
 */
const performJoins = (tables, joins, headerIndices, appends = []) => {
    // 1. Process all raw tables into structured arrays of objects
    const processedTables = {};
    
    tables.forEach(table => {
        const idx = headerIndices[table.id] || 0;
        processedTables[table.id] = processRawData(table.rawData, idx);
    });

    if (tables.length === 0) return { data: [], columns: [] };

    // 1.5. Apply Appends
    appends.forEach(append => {
        const top = processedTables[append.topTableId];
        const bottom = processedTables[append.bottomTableId];
        if (top && bottom) {
            // Only append if headers match exactly
            if (JSON.stringify(top.headers) === JSON.stringify(bottom.headers)) {
                top.rows = top.rows.concat(bottom.rows);
            }
        }
    });

    // Start with the first table as the base
    const table0 = tables[0];
    const table0Data = processedTables[table0.id];

    // If there are no joins, just return the first table's data without table prefixes.
    if (!joins || joins.length === 0) {
        return { data: table0Data.rows, columns: table0Data.headers };
    }

    // Important: The base data MUST start with table 0 prefix to match join logic expectation
    let resultData = table0Data.rows.map(row => {
        const newRow = {};
        Object.keys(row).forEach(key => {
            newRow[`${table0.name}.${key}`] = row[key];
        });
        return newRow;
    });
    
    let resultColumns = table0Data.headers.map(c => `${table0.name}.${c}`);

    // Apply joins sequentially
    joins.forEach(join => {
        const rightTable = processedTables[join.rightTableId];
        if (!rightTable) return;

        const rightTableName = tables.find(t => t.id === join.rightTableId)?.name || 'Unknown';
        const leftTableName = tables.find(t => t.id === join.leftTableId)?.name || table0.name;
        
        // Prepare right data with prefixes
        const rightData = rightTable.rows.map(row => {
            const newRow = {};
            Object.keys(row).forEach(key => {
                newRow[`${rightTableName}.${key}`] = row[key];
            });
            return newRow;
        });
        const rightColumns = rightTable.headers.map(c => `${rightTableName}.${c}`);

        // Determine exact key string
        const leftKey = join.leftKey.includes('.') ? join.leftKey : `${leftTableName}.${join.leftKey}`;
        const rightKey = `${rightTableName}.${join.rightKey}`;

        const newResultData = [];
        const matchedRightIndices = new Set();

        // Hash map for Right Table for O(1) lookup
        const rightMap = new Map();
        rightData.forEach((row, idx) => {
            const keyVal = String(row[rightKey]);
            if (!rightMap.has(keyVal)) rightMap.set(keyVal, []);
            rightMap.get(keyVal).push({ row, idx });
        });

        // Iterate Left Data
        resultData.forEach(leftRow => {
            const keyVal = String(leftRow[leftKey]);
            const matches = rightMap.get(keyVal);

            if (matches && matches.length > 0) {
                matches.forEach(match => {
                    newResultData.push({ ...leftRow, ...match.row });
                    matchedRightIndices.add(match.idx);
                });
            } else {
                // No match found
                if (join.type === 'LEFT' || join.type === 'FULL') {
                    // Fill with nulls for right columns
                    const nullRight = {};
                    rightColumns.forEach(c => nullRight[c] = null);
                    newResultData.push({ ...leftRow, ...nullRight });
                }
            }
        });

        // Handle Right / Full Outer Joins for unmatched right rows
        if (join.type === 'RIGHT' || join.type === 'FULL') {
            rightData.forEach((rightRow, idx) => {
                if (!matchedRightIndices.has(idx)) {
                    // Create null left part
                    const nullLeft = {};
                    resultColumns.forEach(c => nullLeft[c] = null);
                    newResultData.push({ ...nullLeft, ...rightRow });
                }
            });
        }

        resultData = newResultData;
        
        // Merge columns
        const newColSet = new Set([...resultColumns, ...rightColumns]);
        resultColumns = Array.from(newColSet);
    });

    return { data: resultData, columns: resultColumns };
};

module.exports = {
    processRawData,
    performJoins
};
