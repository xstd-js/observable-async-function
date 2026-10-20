import { ObservableAsyncFunction } from './observable-async-function.ts';

declare const source: ObservableAsyncFunction<number | string>;

// boolean predicate -> overload 2 (no narrowing)
const filtered = source.filter((value): boolean => typeof value === 'string');

// type guard -> overload 1 (narrowing to string)
const narrowed = source.filter((value): value is string => typeof value === 'string');

// Both assignments must compile:
const asOriginal: ObservableAsyncFunction<number | string> = filtered;
const asNarrowed: ObservableAsyncFunction<string> = narrowed;

// keep references "used" for noUnusedLocals:
export const usage = [asOriginal, asNarrowed];
