[![npm (scoped)](https://img.shields.io/npm/v/@xstd/observable-async-function.svg)](https://www.npmjs.com/package/@xstd/observable-async-function)
![npm](https://img.shields.io/npm/dm/@xstd/observable-async-function.svg)
![NPM](https://img.shields.io/npm/l/@xstd/observable-async-function.svg)
![npm type definitions](https://img.shields.io/npm/types/@xstd/observable-async-function.svg)
![coverage](https://img.shields.io/badge/coverage-100%25-green)
![AI generation](https://img.shields.io/badge/AI_generation-medium-yellow)

<picture>
  <source height="64" media="(prefers-color-scheme: dark)" srcset="https://github.com/xstd-js/website/blob/main/assets/logo/png/logo-large-dark.png?raw=true">
  <source height="64" media="(prefers-color-scheme: light)" srcset="https://github.com/xstd-js/website/blob/main/assets/logo/png/logo-large-light.png?raw=true">
  <img height="64" alt="Shows a black logo in light color mode and a white one in dark color mode." src="https://github.com/xstd-js/website/blob/main/assets/logo/png/logo-large-light.png?raw=true">
</picture>

## @xstd/observable-async-function

AsyncFunction mixed with ListenerResource.

## 📦 Installation

```shell
yarn add @xstd/observable-async-function
# or
npm install @xstd/observable-async-function --save
```

## 🚀 Usage

```ts
import { ListenerResource } from '@xstd/listener-resource';
import { ObservableAsyncFunction } from '@xstd/observable-async-function';

// A source emitting increasing ticks:
const ticks: ObservableAsyncFunction<number> = new ObservableAsyncFunction<number>(
  async (): Promise<ListenerResource<number>> => {
    return new ListenerResource<number>((emit) => {
      let count: number = 0;
      const interval: ReturnType<typeof setInterval> = setInterval((): void => emit(count++), 100);
      return (): void => clearInterval(interval); // teardown, invoked with the close reason on close
    });
  },
);

// Derive a new observable function with familiar operators:
const events: ObservableAsyncFunction<{ tick: number }> = ticks
  .filter((value: number): boolean => value % 2 === 0)
  .map((value: number): { tick: number } => ({ tick: value }));

// Consume the values (the underlying resource is disposed of automatically):
const controller: AbortController = new AbortController();
for await (const event of events.values(controller.signal)) {
  console.log(event.tick);
  if (event.tick >= 10) break; // the underlying source is closed automatically
}
```

## 📜 Documentation

### `ObservableAsyncFunction`

Represents an asynchronous and abortable function whose call establishes a `ListenerResource` emitting values over time.

It combines [`@xstd/async-function`](https://www.npmjs.com/package/@xstd/async-function) with [`@xstd/listener-resource`](https://www.npmjs.com/package/@xstd/listener-resource): every call establishes a fresh live resource, while the async function toolbox (`then`, `catch`, `mapArguments`, `shareConcurentCalls`...) keeps operating on the function itself.

- **Type parameters**
  - `GValue` — The type of the emitted values.
  - `GArguments extends readonly unknown[] = []` — The tuple type of arguments passed to the function.

The lifecycle contract:

- Every call requires a leading `AbortSignal`, which aborts the _establishment_ of the resource. Once the call resolved, aborting the signal has no effect on the returned resource.
- The resource's lifetime is managed explicitly: through `close()`, the async-disposal protocol (`await using`), or `closesWith()`.
- Closing is bidirectional: closing a derived resource tears down its upstream, and an upstream resource closing cascades to the resources derived from it.
- Like for any `ListenerResource` consumer subscribing after an asynchronous establishment, values emitted by a source _during_ its establishment are not received: sources must start emitting only after their establishment completed.
- The `first`, `values` and `forEach` methods dispose of the resource automatically.

#### Constructor

```ts
new ObservableAsyncFunction<GValue, GArguments>(
  call: AsyncFunctionLike<GArguments, ListenerResource<GValue>>,
)
```

Creates an instance of the `ObservableAsyncFunction` class.

- **Parameters**
  - `call: AsyncFunctionLike<GArguments, ListenerResource<GValue>>` — A call function, or an existing `AsyncFunction`, which establishes the resource. It receives the call's `AbortSignal` as first argument, followed by the call's arguments, and returns the established `ListenerResource` (or a promise of it). If an `AsyncFunction` is provided, its underlying call function is reused instead of being wrapped.

```ts
// from a call function wrapping an EventTarget:
const changes: ObservableAsyncFunction<Event> = new ObservableAsyncFunction<Event>(
  async (): Promise<ListenerResource<Event>> => {
    return new ListenerResource<Event>((emit) => {
      target.addEventListener('change', emit);
      return (): void => target.removeEventListener('change', emit);
    });
  },
);

// from an existing ObservableAsyncFunction (reuses its underlying call function):
const copy = new ObservableAsyncFunction(changes);
```

#### Static methods

##### `ObservableAsyncFunction.merge`

```ts
static merge<GValue, GArguments extends readonly unknown[]>(
  functions: readonly ObservableAsyncFunction<GValue, GArguments>[],
): ObservableAsyncFunction<GValue, GArguments>
```

Merges multiple observable async functions into a single one emitting the values of all of them.

When the merged function is called, all the provided functions are called concurrently with the same signal and arguments, and their resources are subscribed.

- Closing the merged resource closes all the sources with the close reason.
- When all the sources have closed, the merged resource closes too.
- If one of the sources fails to establish (its call rejects or resolves with an already-closed resource), the call rejects with that error and the sources opened so far are closed.
- With an empty array, the merged resource emits nothing and never closes on its own (no source's lifecycle can trigger it): the consumer is responsible for closing it or aborting the call's signal.

- **Parameters**
  - `functions: readonly ObservableAsyncFunction<GValue, GArguments>[]` — The observable async functions to merge.
- **Returns** The merged observable async function.

```ts
const merged: ObservableAsyncFunction<number> = ObservableAsyncFunction.merge([
  temperature,
  humidity,
]);

for await (const value of merged.values(controller.signal)) {
  console.log(value); // values from both sources
}
```

##### `ObservableAsyncFunction.fromEventTarget`

```ts
static fromEventTarget<GEvent extends Event>(
  target: EventTarget,
  type: string,
): ObservableAsyncFunction<GEvent>
```

Creates an observable async function emitting the events of the given type dispatched by the provided `EventTarget`.

Each call registers a new listener on the target, and closing the established resource removes it.

- **Type parameters**
  - `GEvent extends Event` — The type of the dispatched events (e.g. `MouseEvent` for `'click'`).
- **Parameters**
  - `target: EventTarget` — The `EventTarget` to listen to.
  - `type: string` — The type of the events to listen to (e.g. `'click'`).
- **Returns** A new observable async function emitting the dispatched events.

```ts
const clicks: ObservableAsyncFunction<MouseEvent> =
  ObservableAsyncFunction.fromEventTarget<MouseEvent>(button, 'click');

for await (const click of clicks.values(controller.signal)) {
  console.log(click.clientX);
}
```

##### `ObservableAsyncFunction.interval`

```ts
static interval(time: number): ObservableAsyncFunction<void>
```

Creates an observable async function emitting a value at a regular interval.

Each call starts a new interval, and closing the established resource clears it.

- **Parameters**
  - `time: number` — The delay in milliseconds between each emission.
- **Returns** A new observable async function emitting a `void` value every `time` milliseconds.

```ts
const heartbeat: ObservableAsyncFunction<void> = ObservableAsyncFunction.interval(1000);

for await (const _ of heartbeat.values(controller.signal)) {
  console.log('heartbeat'); // every second
}
```

#### Instance methods

The transform operators (`map`, `mapFilter`, `filter`, `distinct`) share the same derivation semantics: each call of the derived function establishes the upstream resource then subscribes to it; the derived resource is closed when its upstream closes, and closing it closes the upstream. The transform callbacks are invoked synchronously for each upstream value and must not throw: the dispatch being fail-fast, a thrown error is rethrown to the upstream emitter, skipping the remaining listeners.

##### `call`

```ts
call(signal: AbortSignal, ...args: GArguments): Promise<ListenerResource<GValue>>
```

Establishes the resource with the provided arguments.

- **Parameters**
  - `signal: AbortSignal` — A signal to abort the establishment.
  - `...args: GArguments` — The arguments to be provided to the call function.
- **Returns** A promise resolving with the established `ListenerResource`. Rejects if the establishment fails or the `signal` aborts during it.

```ts
const listener: ListenerResource<number> = await ticks.call(controller.signal);

listener.listen((value: number): void => console.log(value));

await listener.close(); // tears down the underlying source
```

##### `mapArguments`

```ts
mapArguments<GNewArguments extends readonly unknown[]>(
  mapFnc: MapFunctionArguments<GNewArguments, GArguments>,
): ObservableAsyncFunction<GValue, GNewArguments>
```

Maps the arguments of this observable async function into the arguments of the returned one.

- **Type parameters**
  - `GNewArguments` — The tuple type of the new arguments.
- **Parameters**
  - `mapFnc` — A function receiving the new arguments and returning the arguments for this function.
- **Returns** A new observable async function accepting the mapped arguments.

```ts
const byIndex: ObservableAsyncFunction<number, [number]> = byId.mapArguments(
  (index: number): [string] => [`id-${index}`],
);

const listener = await byIndex.call(signal, 42); // the source receives 'id-42'
```

##### `then`

```ts
then(onFulfilled?: undefined, onRejectedFunction?: undefined): ObservableAsyncFunction<GValue, GArguments>
then<GNewValue>(
  onFulfilled: AsyncFunctionThenOnFulfilledFunction<ListenerResource<GValue>, ListenerResource<GNewValue>>,
  onRejectedFunction?: AsyncFunctionThenOnRejectedFunction<ListenerResource<GNewValue>> | undefined,
): ObservableAsyncFunction<GNewValue, GArguments>
then<GNewValue>(
  onFulfilled: undefined,
  onRejectedFunction: AsyncFunctionThenOnRejectedFunction<ListenerResource<GNewValue>>,
): ObservableAsyncFunction<GValue | GNewValue, GArguments>
```

Chains handlers on the outcome of this function, like `Promise.then`, but returns a new `ObservableAsyncFunction` instead of a `Promise`.

The handlers are applied on each call of the returned function, transforming the established `ListenerResource` into another one. The `AbortSignal` received by the call is forwarded to the handlers. If no handler is provided, the resource or error is passed through unchanged.

- **Type parameters**
  - `GNewValue` — The type of the resource resolved by the returned function.
- **Parameters**
  - `onFulfilled` — A function invoked with the established resource and the call's `AbortSignal`, returning the new resource (or a promise of it).
  - `onRejectedFunction` — A function invoked with the thrown error and the call's `AbortSignal`, returning a fallback resource (or a promise of it). Defaults to re-throwing the error.
- **Returns** A new observable async function resolving with the transformed resource.

```ts
const strings: ObservableAsyncFunction<string> = ticks.then(mapToStringResource);
```

##### `catch`

```ts
catch(onRejectedFunction?: undefined): ObservableAsyncFunction<GValue, GArguments>
catch<GNewValue>(
  onRejectedFunction: AsyncFunctionThenOnRejectedFunction<ListenerResource<GNewValue>>,
): ObservableAsyncFunction<GValue | GNewValue, GArguments>
```

Handles the errors thrown by this function, like `Promise.catch`, but returns a new `ObservableAsyncFunction` instead of a `Promise`.

The rejection handler is applied on each call of the returned function. The `AbortSignal` received by the call is forwarded to the handler. The established resources are passed through unchanged.

- **Type parameters**
  - `GNewValue` — The type of the resource resolved by the returned function in case of error.
- **Parameters**
  - `onRejectedFunction` — A function invoked with the thrown error and the call's `AbortSignal`, returning a fallback resource (or a promise of it). If `undefined`, errors are passed through unchanged.
- **Returns** A new observable async function resolving with the original resource, or the fallback resource when an error occurs.

```ts
const safe: ObservableAsyncFunction<number | string> = ticks.catch(
  (): ListenerResource<number | string> => fallbackResource,
);
```

##### `shareConcurentCalls` (experimental)

```ts
shareConcurentCalls(options: ShareConcurentCallsOptions<GArguments>): ObservableAsyncFunction<GValue, GArguments>
```

Returns a new observable async function that deduplicates concurrent calls sharing the same key: while a call is in-flight, subsequent calls with the same key await the same underlying resource instead of triggering a new call.

- **Parameters** `options` — Configuration object.
  - `keyGenerator: (args: GArguments) => string` — A function that generates a cache key from the call's arguments. Calls producing the same key share their result.
- **Returns** A new observable async function sharing concurrent calls with identical keys.

```ts
const shared = ticks.shareConcurentCalls({ keyGenerator: () => 'ticks' });

// the source is established once, both calls await the same resource:
const [a, b] = await Promise.all([shared.call(signalA), shared.call(signalB)]);
```

##### `map`

```ts
map<GNewValue>(map: MapFunction<GValue, GNewValue>): ObservableAsyncFunction<GNewValue, GArguments>
```

Maps each emitted value through the provided function.

- **Type parameters**
  - `GNewValue` — The type of the emitted values.
- **Parameters**
  - `map` — A function transforming an incoming value into an outgoing value.
- **Returns** A new observable async function emitting the mapped values.

```ts
const lengths: ObservableAsyncFunction<number> = strings.map(
  (value: string): number => value.length,
);
```

##### `mapFilter`

```ts
mapFilter<GNewValue>(
  mapFilter: ObservableAsyncFunctionMapFilterFunction<GValue, GNewValue, GArguments>,
): ObservableAsyncFunction<GNewValue, GArguments>
```

Maps each emitted value through the provided function, dropping the values mapped to `NONE`.

- **Type parameters**
  - `GNewValue` — The type of the emitted values.
- **Parameters**
  - `mapFilter` — A function returning an outgoing value, or `NONE` (from [`@xstd/none`](https://www.npmjs.com/package/@xstd/none)) to filter the value out.
- **Returns** A new observable async function emitting the mapped values which are not `NONE`.

```ts
import { NONE } from '@xstd/none';

const ids: ObservableAsyncFunction<string> = inputs.mapFilter(
  (input: Input): string | None => input.id ?? NONE,
);
```

##### `filter`

```ts
filter<GNewValue extends GValue>(
  filter: ObservableAsyncFunctionFilterFunctionWithSubType<GValue, GNewValue, GArguments>,
): ObservableAsyncFunction<GNewValue, GArguments>
filter(
  filter: ObservableAsyncFunctionFilterFunction<GValue, GArguments>,
): ObservableAsyncFunction<GValue, GArguments>
```

Filters the emitted values through the provided predicate.

- **Type parameters**
  - `GNewValue extends GValue` — The narrowed type of the forwarded values, when the predicate is a type guard.
- **Parameters**
  - `filter` — A predicate. Values for which it returns `false` are filtered out. Providing a type-guard predicate narrows the emitted values.
- **Returns** A new observable async function emitting only the values satisfying the predicate.

```ts
// with a simple predicate:
const positive: ObservableAsyncFunction<number> = numbers.filter(
  (value: number): boolean => value > 0,
);

// with a type-guard predicate, the emitted values are narrowed:
const strings: ObservableAsyncFunction<string> = values.filter(
  (value: number | string): value is string => typeof value === 'string',
);
```

##### `distinct`

```ts
distinct(equal?: EqualFunction<GValue>): ObservableAsyncFunction<GValue, GArguments>
```

Filters out the consecutive values considered equal to the previously emitted one.

The equality state is maintained _per subscription_: each call of the returned function gets its own deduplication state, and the first value received by a subscription is always emitted.

- **Parameters**
  - `equal` — The equality function applied to consecutive values. Defaults to strict equality (`===`).
- **Returns** A new observable async function emitting only the values differing from the previous one.

```ts
const temperatures: ObservableAsyncFunction<number> = sensorValues.distinct();

// with a custom equality function:
const pages: ObservableAsyncFunction<number> = navigationEvents.distinct(
  (a: number, b: number): boolean => Math.floor(a) === Math.floor(b),
);
```

##### `first`

```ts
first(signal: AbortSignal, ...args: GArguments): Promise<GValue>
```

Resolves with the first value emitted by a new resource established from this function.

The established resource is disposed of as soon as the first value is received, the `signal` aborts, or the resource closes. The returned promise rejects if the `signal` aborts or the resource closes before any value is emitted.

- **Parameters**
  - `signal` — A signal to abort the establishment and the wait.
  - `...args: GArguments` — The arguments passed to the function.
- **Returns** A promise resolving with the first emitted value.

```ts
const firstTemperature: number = await sensorValues.first(controller.signal);
```

##### `values`

```ts
values(
  input: AbortSignal | ObservableAsyncFunctionToIteratorOptions,
  ...args: GArguments,
): AsyncGenerator<GValue>
```

Iterates over the values emitted by a new resource established from this function.

The established resource is disposed of when the iteration ends (including `break` and thrown errors), when the provided `signal` aborts, or when the resource closes.

Values emitted while the consumer is busy processing the previous one are dropped according to the `queueingPolicy` option, which defaults to `EdgeQueuingPolicy`. Provide a `queueingPolicy` to buffer them instead.

Closing the resource mid-iteration rejects the pending iteration with the close reason: the iteration only ends cleanly when the consumer breaks out of it.

- **Parameters**
  - `input` — An `AbortSignal`, or the iteration options.
  - `...args: GArguments` — The arguments passed to the function.
- **Returns** An async generator yielding the emitted values.

```ts
for await (const value of ticks.values(controller.signal)) {
  if (value > LIMIT) break; // the underlying source is closed automatically
}

// with an options object:
for await (const value of ticks.values({ signal: controller.signal, queueingPolicy })) {
  console.log(value);
}
```

##### `forEach`

```ts
forEach(
  options: AbortSignal | ObservableAsyncFunctionToIteratorOptions,
  callback: (value: GValue) => PromiseLike<void> | void,
  ...args: GArguments,
): Promise<void>
```

Invokes the provided callback for each value emitted by a new resource established from this function.

The established resource is disposed of when the callback throws, when the provided `signal` aborts, or when the resource closes. Like for `values`, values emitted while the callback is still running are dropped according to the `queueingPolicy` (defaulting to `EdgeQueuingPolicy`).

- **Parameters**
  - `options` — An `AbortSignal`, or the iteration options.
  - `callback` — A function invoked for each emitted value. May be asynchronous.
  - `...args: GArguments` — The arguments passed to the function.
- **Returns** A promise which rejects when the iteration ends: with the callback's error when it throws, with the close reason when the resource closes, or with the abort reason when the signal aborts. It never resolves on its own: only a consumer-side termination ends the iteration cleanly, which `forEach` cannot trigger.

```ts
await ticks.forEach(controller.signal, (value: number): void => console.log(value));
```

#### Types

| Type                                                                                  | Description                                                                                                 |
| ------------------------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------- |
| `ObservableAsyncFunctionMapFilterFunction<GIn, GOut, GArguments>`                     | `(value: GIn, ...args: GArguments) => GOut \| None` — maps a value, or returns `NONE` to filter it out.     |
| `ObservableAsyncFunctionFilterFunction<GValue, GArguments>`                           | `(value: GValue, ...args: GArguments) => boolean` — predicate deciding whether a value is forwarded.        |
| `ObservableAsyncFunctionFilterFunctionWithSubType<GIn, GOut extends GIn, GArguments>` | `(value: GIn, ...args: GArguments) => value is GOut` — type-guard predicate narrowing the forwarded values. |
| `ObservableAsyncFunctionToIteratorOptions`                                            | `ListenerResourceTOIteratorOptions` with a required `signal` — the options of `values` and `forEach`.       |

### 📚 Full example

```ts
import { ListenerResource } from '@xstd/listener-resource';
import { ObservableAsyncFunction } from '@xstd/observable-async-function';

// A source emitting increasing ticks:
const ticks: ObservableAsyncFunction<number> = new ObservableAsyncFunction<number>(
  async (): Promise<ListenerResource<number>> => {
    return new ListenerResource<number>((emit) => {
      let count: number = 0;
      const interval: ReturnType<typeof setInterval> = setInterval((): void => emit(count++), 100);
      return (): void => clearInterval(interval); // teardown, invoked with the close reason on close
    });
  },
);

// Derive a new observable function:
const events: ObservableAsyncFunction<{ tick: number }> = ticks
  .filter((value: number): boolean => value % 2 === 0)
  .map((value: number): { tick: number } => ({ tick: value }));

// Consume the values:
const controller: AbortController = new AbortController();
for await (const event of events.values(controller.signal)) {
  console.log(event.tick);
  if (event.tick >= 10) break; // the underlying source is closed automatically
}
```
