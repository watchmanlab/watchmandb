function isArray(value: unknown): value is unknown[] {
  return Array.isArray(value);
}

function isNull(value: unknown): value is null {
  return isEqual(value, null);
}

function isObject(value: unknown): value is Record<PropertyKey, unknown> {
  return isEqual(typeof value, "object") && !isNull(value);
}

function isEqual(left: unknown, right: unknown): boolean {
  return left === right;
}

function getKeys(value: Record<PropertyKey, unknown>): string[] {
  return Object.getOwnPropertyNames(value);
}

function isDeepEqualObject(
  left: Record<PropertyKey, unknown>,
  right: unknown,
): boolean {
  if (!isObject(right)) return false;
  const keys = getKeys(left);
  return (
    isEqual(keys.length, getKeys(right).length) &&
    keys.every((key) => isDeepEqual(left[key], right[key]))
  );
}
function isDeepEqualArray(left: unknown[], right: unknown): boolean {
  return (
    isArray(right) &&
    isEqual(left.length, right.length) &&
    left.every((_, index) => isDeepEqual(left[index], right[index]))
  );
}

export function isDeepEqual(left: unknown, right: unknown): boolean {
  return isArray(left)
    ? isDeepEqualArray(left, right)
    : isObject(left)
      ? isDeepEqualObject(left, right)
      : isEqual(left, right);
}
