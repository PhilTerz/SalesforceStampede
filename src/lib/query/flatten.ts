/**
 * Flatten Salesforce records for grid display.
 * Handles nested relationships and removes `attributes` metadata.
 */

const MAX_DEPTH = 5;

/**
 * Flattens a single Salesforce record.
 * - Removes `attributes` keys at all levels
 * - Flattens nested objects into dot-notation keys (e.g., Owner.Name)
 * - Stringifies arrays and deeply nested objects
 */
export function flattenRecord(
  input: unknown,
  prefix = '',
  depth = 0
): Record<string, unknown> {
  const result: Record<string, unknown> = {};

  if (input === null || input === undefined) {
    return result;
  }

  if (typeof input !== 'object') {
    return result;
  }

  const obj = input as Record<string, unknown>;

  for (const [key, value] of Object.entries(obj)) {
    // Skip the attributes metadata key
    if (key === 'attributes') {
      continue;
    }

    const fullKey = prefix ? `${prefix}.${key}` : key;

    if (value === null || value === undefined) {
      result[fullKey] = value;
    } else if (Array.isArray(value)) {
      // For arrays, stringify or join primitives
      if (value.length === 0) {
        result[fullKey] = '';
      } else if (value.every((v) => typeof v !== 'object' || v === null)) {
        // Array of primitives - join them
        result[fullKey] = value.join(', ');
      } else {
        // Array of objects - stringify
        result[fullKey] = JSON.stringify(value);
      }
    } else if (typeof value === 'object') {
      // Nested object - recurse if within depth limit
      if (depth < MAX_DEPTH) {
        const nested = flattenRecord(value, fullKey, depth + 1);
        Object.assign(result, nested);
      } else {
        // Beyond max depth - stringify
        result[fullKey] = JSON.stringify(value);
      }
    } else {
      // Primitive value
      result[fullKey] = value;
    }
  }

  return result;
}

/**
 * Flattens an array of Salesforce records.
 */
export function flattenRecords(records: unknown[]): Record<string, unknown>[] {
  return records.map((r) => flattenRecord(r));
}

/**
 * Extracts column keys from flattened records.
 * Uses the first N records to determine stable column list.
 */
export function extractColumnKeys(
  records: Record<string, unknown>[],
  sampleSize = 50
): string[] {
  const keySet = new Set<string>();

  const sample = records.slice(0, sampleSize);
  for (const record of sample) {
    for (const key of Object.keys(record)) {
      keySet.add(key);
    }
  }

  // Sort keys: Id first, then alphabetically
  const keys = Array.from(keySet);
  keys.sort((a, b) => {
    if (a === 'Id') return -1;
    if (b === 'Id') return 1;
    return a.localeCompare(b);
  });

  return keys;
}

/**
 * Checks if a value looks like a Salesforce ID.
 * Salesforce IDs are 15 or 18 alphanumeric characters.
 */
export function isSalesforceId(value: unknown): boolean {
  if (typeof value !== 'string') return false;
  // Salesforce IDs: 15 or 18 chars, alphanumeric
  return /^[a-zA-Z0-9]{15}$|^[a-zA-Z0-9]{18}$/.test(value);
}

/**
 * Checks if a column key represents an ID field.
 * ID fields are exactly "Id" or end with "Id".
 */
export function isIdColumn(key: string): boolean {
  return key === 'Id' || key.endsWith('Id');
}
