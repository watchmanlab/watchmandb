import type { Key, RangeOptions } from "lmdb";

export class Operator<ROW extends Record<string, any>> {
  constructor(
    public readonly rangeOptions: RangeOptions,
    public readonly predicate: (rowValue: ROW) => boolean,
  ) {}
}

export const gt = <
  ROW extends Record<string, any>,
  T extends Exclude<Key, Symbol>,
>(
  target: T,
) => {
  return new Operator(
    { start: [target, "\xff"], end: undefined, reverse: false },
    (rowValue) => (target ? rowValue > target : true),
  );
};

export type OperatorFactory<ROW extends Record<string, any>, T extends Key> = (
  target: T,
) => Operator<ROW>;
// export const q = {
//   gt: <T>(target: T): OperatorPayload<T> => ({
//     __watchman_op: true,
//     rangeOptions: () => ({
//       start: [target, Symbol.for("last")],
//       end: undefined,
//       reverse: false,
//     }),
//     memoryOps: (rowValue) => rowValue > target,
//   }),

//   gte: <T>(target: T): OperatorPayload<T> => ({
//     __watchman_op: true,
//     rangeOptions: () => ({ start: [target], end: undefined, reverse: false }),
//     memoryOps: (rowValue) => rowValue >= target,
//   }),

//   lt: <T>(target: T): OperatorPayload<T> => ({
//     __watchman_op: true,
//     rangeOptions: () => ({ start: [target], end: undefined, reverse: true }),
//     memoryOps: (rowValue) => rowValue < target,
//   }),

//   lte: <T>(target: T): OperatorPayload<T> => ({
//     __watchman_op: true,
//     rangeOptions: () => ({
//       start: [target, Symbol.for("last")],
//       end: undefined,
//       reverse: true,
//     }),
//     memoryOps: (rowValue) => rowValue <= target,
//   }),

//   between: <T>(min: T, max: T): OperatorPayload<T> => ({
//     __watchman_op: true,
//     rangeOptions: () => ({
//       start: [min],
//       end: [max, Symbol.for("last")],
//       reverse: false,
//     }),
//     memoryOps: (rowValue) => rowValue >= min && rowValue <= max,
//   }),
// };
