import {
  AsyncFunction,
  type AsyncFunctionThenOnFulfilledFunction,
  type AsyncFunctionThenOnRejectedFunction,
  type ShareConcurentCallsOptions,
} from '@xstd/async-function';
import { EQUAL_FUNCTION_STRICT_EQUAL, type EqualFunction } from '@xstd/equal-function';
import {
  type EmitValue,
  ListenerResource,
  type ListenerResourceTOIteratorOptions,
} from '@xstd/listener-resource';
import type { MapFunction, MapFunctionArguments } from '@xstd/map-function';
import { NONE, type None } from '@xstd/none';
import type { OnCloseResource } from '@xstd/resource';

/**
 * A function transforming the values emitted by an upstream `ListenerResource` into values emitted downstream.
 *
 * Called synchronously for each upstream value.
 *
 * @template GIn - The type of the upstream values.
 * @template GOut - The type of the downstream values.
 * @template GArguments - The tuple type of the arguments passed to the observable async function.
 * @param value - The upstream value.
 * @param emit - A function to broadcast the transformed value downstream.
 * @param args - The arguments passed to the derived function's call.
 */
interface ObservableAsyncFunctionTransformOutputFunction<
  GIn,
  GOut,
  GArguments extends readonly unknown[],
> {
  (value: GIn, emit: EmitValue<GOut>, args: GArguments): void;
}

/**
 * A function mapping an incoming value to an outgoing value, or to {@link NONE} to drop it.
 *
 * @template GIn - The type of the incoming values.
 * @template GOut - The type of the outgoing values.
 * @template GArguments - The tuple type of the extra arguments passed to the function.
 * @param value - The incoming value.
 * @param args - The extra arguments provided at call time.
 * @returns The outgoing value, or {@link NONE} to filter the value out.
 */
export interface ObservableAsyncFunctionMapFilterFunction<
  GIn,
  GOut,
  GArguments extends readonly unknown[],
> {
  (value: GIn, ...args: GArguments): GOut | None;
}

/**
 * A predicate deciding whether an incoming value is forwarded downstream.
 *
 * @template GValue - The type of the incoming values.
 * @template GArguments - The tuple type of the extra arguments passed to the function.
 * @param value - The incoming value.
 * @param args - The extra arguments provided at call time.
 * @returns `true` to forward the value, `false` to filter it out.
 */
export interface ObservableAsyncFunctionFilterFunction<
  GValue,
  GArguments extends readonly unknown[],
> {
  (value: GValue, ...args: GArguments): boolean;
}

/**
 * A type-guard predicate deciding whether an incoming value is forwarded downstream, narrowing its type.
 *
 * @template GIn - The type of the incoming values.
 * @template GOut - The narrowed type of the forwarded values.
 * @template GArguments - The tuple type of the extra arguments passed to the function.
 * @param value - The incoming value.
 * @param args - The extra arguments provided at call time.
 * @returns A type predicate: forwarded values are narrowed to `GOut`.
 */
export interface ObservableAsyncFunctionFilterFunctionWithSubType<
  GIn,
  GOut extends GIn,
  GArguments extends readonly unknown[],
> {
  (value: GIn, ...args: GArguments): value is GOut;
}

/**
 * Options of the `ObservableAsyncFunction.values` and `ObservableAsyncFunction.forEach` methods.
 *
 * @template signal - An `AbortSignal` stopping the iteration when aborted.
 * @template queueingPolicy - Optional. Controls the buffering of the values emitted while the consumer is busy processing the previous one. Defaults to `EdgeQueuingPolicy`, which keeps only the latest value.
 */
export interface ObservableAsyncFunctionToIteratorOptions extends ListenerResourceTOIteratorOptions {
  readonly signal: AbortSignal;
}

/**
 * An `AsyncFunction` whose call resolves with a live `ListenerResource` emitting values over time.
 *
 * Calling the function establishes the resource and returns it; the received `AbortSignal` aborts the
 * _establishment_ of the resource only. Once established, the resource's lifetime is managed explicitly:
 * through `close()`, the async-disposal protocol (`await using`), or `closesWith()`. In particular,
 * aborting the call's signal after the call resolved does _not_ close the returned resource.
 *
 * Closing the returned resource tears down its upstream (e.g. the source it was derived from), and
 * closing an upstream resource cascades to the resources derived from it.
 *
 * Like for any `ListenerResource` consumer subscribing after an asynchronous establishment, values
 * emitted by a source _during_ its establishment are not received: sources must start emitting only
 * after their establishment completed.
 *
 * The `first`, `values` and `forEach` methods dispose of the resource automatically.
 *
 * @example Establishing a resource and closing it
 * ```ts
 * const source: ObservableAsyncFunction<number> = /* ... *\/;
 *
 * const controller: AbortController = new AbortController();
 * const listener: ListenerResource<number> = await source.call(controller.signal);
 *
 * listener.listen((value: number): void => console.log(value));
 *
 * await listener.close(); // tears down the underlying source
 * ```
 *
 * @example Consuming values without manual disposal
 * ```ts
 * for await (const value of source.values(controller.signal)) {
 *   console.log(value); // `break`ing disposes the underlying source automatically
 * }
 * ```
 */
export class ObservableAsyncFunction<
  GValue,
  GArguments extends readonly unknown[] = [],
> extends AsyncFunction<GArguments, ListenerResource<GValue>> {
  /**
   * Merges multiple observable async functions into a single one emitting the values of all of them.
   *
   * When the merged function is called, all the provided functions are called concurrently with the same
   * arguments, and their resources are subscribed. The returned resource emits every value emitted by any
   * of the sources.
   *
   * The merged resource is tied to the sources' lifecycles in both directions:
   * - closing the merged resource closes all the sources with the close reason;
   * - when all the sources have closed, the merged resource closes too.
   *
   * If one of the sources fails to establish (its call rejects or resolves with a closed resource), the
   * call rejects with that error and all the sources opened so far are closed.
   *
   * @template GValue - The type of the emitted values.
   * @template GArguments - The tuple type of the arguments passed to the functions.
   * @param functions - The observable async functions to merge. When empty, the merged resource emits
   * nothing and never closes on its own (no source's lifecycle can trigger it): the consumer is
   * responsible for closing it or aborting the call's signal.
   * @returns The merged observable async function.
   *
   * @example
   * ```ts
   * const merged: ObservableAsyncFunction<number> = ObservableAsyncFunction.merge([ticks, events]);
   *
   * for await (const value of merged.values(controller.signal)) {
   *   console.log(value); // values from both 'ticks' and 'events'
   * }
   * ```
   */
  static merge<GValue, GArguments extends readonly unknown[]>(
    functions: readonly ObservableAsyncFunction<GValue, GArguments>[],
  ): ObservableAsyncFunction<GValue, GArguments> {
    return new ObservableAsyncFunction<GValue, GArguments>(
      async (signal: AbortSignal, ...args: GArguments): Promise<ListenerResource<GValue>> => {
        const sharedController: AbortController = new AbortController();
        const sharedSignal: AbortSignal = AbortSignal.any([sharedController.signal, signal]);

        const listeners: ListenerResource<GValue>[] = [];

        try {
          await Promise.all(
            functions.map(
              async (fnc: ObservableAsyncFunction<GValue, GArguments>): Promise<void> => {
                listeners.push(await fnc.call(sharedSignal, ...args));
              },
            ),
          );

          for (const listener of listeners) {
            listener.throwIfClosed();
          }
        } catch (error: unknown) {
          sharedController.abort(error);

          await Promise.allSettled(
            listeners.map((listener: ListenerResource<GValue>): Promise<void> => {
              return listener.close(error);
            }),
          );

          throw error;
        }

        return new ListenerResource<GValue>((emit: EmitValue<GValue>): OnCloseResource | void => {
          for (const listener of listeners) {
            listener.listen((value: GValue): void => {
              emit(value);
            });
          }

          return async (reason: unknown): Promise<void> => {
            await Promise.all(
              listeners.map((listener: ListenerResource<GValue>): Promise<void> => {
                return listener.close(reason);
              }),
            );
          };
        }).closesWith(
          AbortSignal.any(
            listeners.map((listener: ListenerResource<GValue>): AbortSignal => {
              return listener.closeSignal;
            }),
          ),
        );
      },
    );
  }

  /**
   * Creates an observable async function emitting the events of the given type dispatched by the provided `EventTarget`.
   *
   * Each call registers a new listener on the target, and closing the established resource removes it.
   *
   * @template GEvent - The type of the dispatched events (e.g. `MouseEvent` for `'click'`).
   * @param target - The `EventTarget` to listen to.
   * @param type - The type of the events to listen to (e.g. `'click'`).
   * @returns A new observable async function emitting the dispatched events.
   *
   * @example
   * ```ts
   * const clicks: ObservableAsyncFunction<MouseEvent> =
   *   ObservableAsyncFunction.fromEventTarget<MouseEvent>(button, 'click');
   *
   * for await (const click of clicks.values(controller.signal)) {
   *   console.log(click.clientX);
   * }
   * ```
   */
  static fromEventTarget<GEvent extends Event>(
    target: EventTarget,
    type: string,
  ): ObservableAsyncFunction<GEvent> {
    return new ObservableAsyncFunction<GEvent>((): ListenerResource<GEvent> => {
      return new ListenerResource<GEvent>((emit: EmitValue<GEvent>): OnCloseResource => {
        target.addEventListener(type, emit as EventListenerOrEventListenerObject);
        return (): void =>
          target.removeEventListener(type, emit as EventListenerOrEventListenerObject);
      });
    });
  }

  /**
   * Creates an observable async function emitting a value at a regular interval.
   *
   * Each call starts a new interval, and closing the established resource clears it.
   *
   * @param time - The delay in milliseconds between each emission.
   * @returns A new observable async function emitting a `void` value every `time` milliseconds.
   *
   * @example
   * ```ts
   * const heartbeat: ObservableAsyncFunction<void> = ObservableAsyncFunction.interval(1000);
   *
   * for await (const _ of heartbeat.values(controller.signal)) {
   *   console.log('heartbeat'); // every second
   * }
   * ```
   */
  static interval(time: number): ObservableAsyncFunction<void> {
    return new ObservableAsyncFunction<void>((): ListenerResource<void> => {
      return new ListenerResource<void>((emit: EmitValue<void>): OnCloseResource => {
        const intervalId: ReturnType<typeof setInterval> = setInterval(emit, time);
        return (): void => clearInterval(intervalId);
      });
    });
  }

  /**
   * Maps the arguments of this observable async function into the arguments of the returned one.
   *
   * @template GNewArguments - The tuple type of the new arguments.
   * @param mapFnc - A function receiving the new arguments and returning the arguments for this function.
   * @returns A new observable async function accepting the mapped arguments.
   */
  override mapArguments<GNewArguments extends readonly unknown[]>(
    mapFnc: MapFunctionArguments<GNewArguments, GArguments>,
  ): ObservableAsyncFunction<GValue, GNewArguments> {
    return new ObservableAsyncFunction<GValue, GNewArguments>(super.mapArguments(mapFnc));
  }

  /**
   * Transforms the resource resolved by this function, and optionally handles its errors.
   *
   * @template GNewValue - The type of the resource resolved by the returned function.
   * @param onFulfilled - A function invoked with the resolved resource and the call's `AbortSignal`, returning the new resource (or a promise of it).
   * @param onRejectedFunction - An optional function invoked with the thrown error and the call's `AbortSignal`, returning a fallback resource (or a promise of it). Defaults to re-throwing the error.
   * @returns A new observable async function resolving with the transformed resource.
   */
  override then(
    onFulfilled?: undefined,
    onRejectedFunction?: undefined,
  ): ObservableAsyncFunction<GValue, GArguments>;
  /**
   * Transforms the resource resolved by this function, and optionally handles its errors.
   *
   * @template GNewValue - The type of the resource resolved by the returned function.
   * @param onFulfilled - A function invoked with the resolved resource and the call's `AbortSignal`, returning the new resource (or a promise of it).
   * @param onRejectedFunction - An optional function invoked with the thrown error and the call's `AbortSignal`, returning a fallback resource (or a promise of it). Defaults to re-throwing the error.
   * @returns A new observable async function resolving with the transformed resource.
   */
  override then<GNewValue>(
    onFulfilled: AsyncFunctionThenOnFulfilledFunction<
      ListenerResource<GValue>,
      ListenerResource<GNewValue>
    >,
    onRejectedFunction?:
      AsyncFunctionThenOnRejectedFunction<ListenerResource<GNewValue>> | undefined,
  ): ObservableAsyncFunction<GNewValue, GArguments>;
  /**
   * Handles the errors thrown by this function without transforming its resolved resource.
   *
   * @template GNewValue - The type of the resource resolved by the returned function in case of error.
   * @param onFulfilled - Must be left `undefined`.
   * @param onRejectedFunction - A function invoked with the thrown error and the call's `AbortSignal`, returning a fallback resource (or a promise of it).
   * @returns A new observable async function resolving with the original resource, or the fallback resource when an error occurs.
   */
  override then<GNewValue>(
    onFulfilled: undefined,
    onRejectedFunction: AsyncFunctionThenOnRejectedFunction<ListenerResource<GNewValue>>,
  ): ObservableAsyncFunction<GValue | GNewValue, GArguments>;
  override then<GNewValue>(
    onFulfilled?:
      | AsyncFunctionThenOnFulfilledFunction<ListenerResource<GValue>, ListenerResource<GNewValue>>
      | undefined,
    onRejectedFunction?:
      AsyncFunctionThenOnRejectedFunction<ListenerResource<GNewValue>> | undefined,
  ): ObservableAsyncFunction<GValue | GNewValue, GArguments> {
    if (onFulfilled === undefined) {
      return onRejectedFunction === undefined
        ? new ObservableAsyncFunction<GValue, GArguments>(super.then())
        : new ObservableAsyncFunction<GValue | GNewValue, GArguments>(
            super.then(undefined, onRejectedFunction),
          );
    }

    return new ObservableAsyncFunction<GNewValue, GArguments>(
      super.then(onFulfilled, onRejectedFunction),
    );
  }

  /**
   * Returns a new observable async function with the same behaviour as this one (no rejection handler provided).
   *
   * @returns A new observable async function equivalent to this one.
   */
  override catch(onRejectedFunction?: undefined): ObservableAsyncFunction<GValue, GArguments>;
  /**
   * Handles the errors thrown by this function, like `Promise.catch`, but returns a new `ObservableAsyncFunction` instead of a `Promise`.
   *
   * @template GNewValue - The type of the resource resolved by the returned function in case of error.
   * @param onRejectedFunction - A function invoked with the thrown error and the call's `AbortSignal`, returning a fallback resource (or a promise of it).
   * @returns A new observable async function resolving with the original resource, or the fallback resource when an error occurs.
   */
  override catch<GNewValue>(
    onRejectedFunction: AsyncFunctionThenOnRejectedFunction<ListenerResource<GNewValue>>,
  ): ObservableAsyncFunction<GValue | GNewValue, GArguments>;
  override catch<GNewValue>(
    onRejectedFunction?:
      AsyncFunctionThenOnRejectedFunction<ListenerResource<GNewValue>> | undefined,
  ): ObservableAsyncFunction<GValue | GNewValue, GArguments> {
    return new ObservableAsyncFunction<GValue | GNewValue, GArguments>(
      onRejectedFunction === undefined ? super.catch() : super.catch(onRejectedFunction),
    );
  }

  /**
   * Returns a new observable async function that deduplicates concurrent calls sharing the same key: while a call is in-flight, subsequent calls with the same key await the same underlying resource instead of triggering a new call.
   *
   * @param options - Configuration object.
   * @param options.keyGenerator - A function that generates a cache key from the call's arguments. Calls producing the same key share their result.
   * @returns A new observable async function sharing concurrent calls with identical keys.
   */
  override shareConcurentCalls(
    options: ShareConcurentCallsOptions<GArguments>,
  ): ObservableAsyncFunction<GValue, GArguments> {
    return new ObservableAsyncFunction<GValue, GArguments>(super.shareConcurentCalls(options));
  }

  /* ListenerResource  */

  /**
   * Derives a new observable async function by transforming this one's emitted values.
   *
   * The derived resource emits the values produced by `transform`, is closed when its upstream resource
   * closes, and closing it closes the upstream resource.
   *
   * The `transform` function is invoked synchronously for each upstream value: it must not throw.
   * The dispatch being fail-fast, a thrown error is rethrown to the upstream emitter, skipping the
   * remaining listeners.
   *
   * @template GNewValue - The type of the emitted values.
   * @param transform - A function transforming an upstream value into zero or more downstream values.
   * @returns The derived observable async function.
   */
  #transformOutput<GNewValue>(
    transform: ObservableAsyncFunctionTransformOutputFunction<GValue, GNewValue, GArguments>,
  ): ObservableAsyncFunction<GNewValue, GArguments> {
    return new ObservableAsyncFunction<GNewValue, GArguments>(
      async (signal: AbortSignal, ...args: GArguments): Promise<ListenerResource<GNewValue>> => {
        const listener: ListenerResource<GValue> = await this.call(signal, ...args);

        listener.throwIfClosed();

        return new ListenerResource<GNewValue>(
          (emit: EmitValue<GNewValue>): OnCloseResource | void => {
            listener.listen((value: GValue): void => {
              transform(value, emit, args);
            });

            return listener.close.bind(listener);
          },
        ).closesWith(listener);
      },
    );
  }

  /**
   * Maps each emitted value through the provided function.
   *
   * @template GNewValue - The type of the emitted values.
   * @param map - A function transforming an incoming value into an outgoing value.
   * @returns A new observable async function emitting the mapped values.
   *
   * @example
   * ```ts
   * const lengths: ObservableAsyncFunction<number> = strings.map((value: string): number => value.length);
   * ```
   */
  map<GNewValue>(
    map: MapFunction<GValue, GNewValue>,
  ): ObservableAsyncFunction<GNewValue, GArguments> {
    return this.#transformOutput((value: GValue, emit: EmitValue<GNewValue>): void => {
      emit(map(value));
    });
  }

  /**
   * Maps each emitted value through the provided function, dropping the values mapped to {@link NONE}.
   *
   * @template GNewValue - The type of the emitted values.
   * @param mapFilter - A function returning an outgoing value, or {@link NONE} to filter the value out.
   * @returns A new observable async function emitting the mapped values which are not {@link NONE}.
   *
   * @example
   * ```ts
   * const ids: ObservableAsyncFunction<string> = inputs.mapFilter((input: Input): string | None =>
   *   input.id ?? NONE,
   * );
   * ```
   */
  mapFilter<GNewValue>(
    mapFilter: ObservableAsyncFunctionMapFilterFunction<GValue, GNewValue, GArguments>,
  ): ObservableAsyncFunction<GNewValue, GArguments> {
    return this.#transformOutput(
      (value: GValue, emit: EmitValue<GNewValue>, args: GArguments): void => {
        const newValue: GNewValue | None = mapFilter(value, ...args);

        if (newValue !== NONE) {
          emit(newValue);
        }
      },
    );
  }

  /**
   * Filters the emitted values through the provided type-guard predicate, narrowing them.
   *
   * @template GNewValue - The narrowed type of the emitted values.
   * @param filter - A type-guard predicate. Values for which it returns `false` are filtered out.
   * @returns A new observable async function emitting only the values satisfying the predicate, narrowed to `GNewValue`.
   *
   * @example
   * ```ts
   * const strings: ObservableAsyncFunction<string> = values.filter(
   *   (value: number | string): value is string => typeof value === 'string',
   * );
   * ```
   */
  filter<GNewValue extends GValue>(
    filter: ObservableAsyncFunctionFilterFunctionWithSubType<GValue, GNewValue, GArguments>,
  ): ObservableAsyncFunction<GNewValue, GArguments>;
  /**
   * Filters the emitted values through the provided predicate.
   *
   * @param filter - A predicate. Values for which it returns `false` are filtered out.
   * @returns A new observable async function emitting only the values satisfying the predicate.
   *
   * @example
   * ```ts
   * const positive: ObservableAsyncFunction<number> = numbers.filter((value: number): boolean => value > 0);
   * ```
   */
  filter(
    filter: ObservableAsyncFunctionFilterFunction<GValue, GArguments>,
  ): ObservableAsyncFunction<GValue, GArguments>;
  filter<GNewValue extends GValue>(
    filter: ObservableAsyncFunctionFilterFunctionWithSubType<GValue, GNewValue, GArguments>,
  ): ObservableAsyncFunction<GNewValue, GArguments> {
    return this.#transformOutput(
      (value: GValue, emit: EmitValue<GNewValue>, args: GArguments): void => {
        if (filter(value, ...args)) {
          emit(value);
        }
      },
    );
  }

  /**
   * Filters out the consecutive values considered equal to the previous emitted one.
   *
   * The equality state is maintained _per subscription_: each call of the returned function gets its own
   * deduplication state, and the first value received by a subscription is always emitted.
   *
   * @param equal - The equality function applied to consecutive values. Defaults to strict equality (`===`).
   * @returns A new observable async function emitting only the values differing from the previous one.
   *
   * @example
   * ```ts
   * const temperatures: ObservableAsyncFunction<number> = sensorValues.distinct();
   * ```
   */
  distinct(
    equal: EqualFunction<GValue> = EQUAL_FUNCTION_STRICT_EQUAL,
  ): ObservableAsyncFunction<GValue, GArguments> {
    return new ObservableAsyncFunction<GValue, GArguments>(
      async (signal: AbortSignal, ...args: GArguments): Promise<ListenerResource<GValue>> => {
        const listener: ListenerResource<GValue> = await this.call(signal, ...args);

        let lastValue: GValue | None = NONE;

        return new ListenerResource<GValue>((emit: EmitValue<GValue>): OnCloseResource | void => {
          listener.listen((value: GValue): void => {
            if (lastValue === NONE || !equal(value, lastValue)) {
              lastValue = value;
              emit(value);
            }
          });

          return listener.close.bind(listener);
        }).closesWith(listener);
      },
    );
  }

  /**
   * Resolves with the first value emitted by a new resource established from this function.
   *
   * The established resource is disposed of as soon as the first value is received, the call's `signal`
   * aborts, or the resource closes. The returned promise rejects if the `signal` aborts or the resource
   * closes before any value is emitted.
   *
   * @param signal - A signal to abort the establishment and the wait.
   * @param args - The arguments passed to the function.
   * @returns A promise resolving with the first emitted value.
   *
   * @example
   * ```ts
   * const firstTemperature: number = await sensorValues.first(controller.signal);
   * ```
   */
  async first(signal: AbortSignal, ...args: GArguments): Promise<GValue> {
    await using listener: ListenerResource<GValue> = await this.call(signal, ...args);

    return await listener.next({ signal });
  }

  /**
   * Iterates over the values emitted by a new resource established from this function.
   *
   * The established resource is disposed of when the iteration ends (including `break` and thrown
   * errors), when the provided `signal` aborts, or when the resource closes.
   *
   * Values emitted while the consumer is busy processing the previous one are dropped according to the
   * `queueingPolicy`, which defaults to `EdgeQueuingPolicy`. Provide a `queueingPolicy` to buffer them
   * instead.
   *
   * Closing the resource mid-iteration rejects the pending iteration with the close reason: the
   * iteration only ends cleanly when the consumer breaks out of it.
   *
   * @param input - An `AbortSignal`, or the iteration options.
   * @param args - The arguments passed to the function.
   * @returns An async generator yielding the emitted values.
   *
   * @example
   * ```ts
   * for await (const value of source.values(controller.signal)) {
   *   if (value > LIMIT) break; // the underlying source is closed automatically
   * }
   * ```
   */
  async *values(
    input: AbortSignal | ObservableAsyncFunctionToIteratorOptions,
    ...args: GArguments
  ): AsyncGenerator<GValue> {
    let signal: AbortSignal;
    let options: Omit<ObservableAsyncFunctionToIteratorOptions, 'signal'>;

    if (input instanceof AbortSignal) {
      signal = input;
      options = {};
    } else {
      signal = input.signal;
      options = input;
    }

    await using listener: ListenerResource<GValue> = await this.call(signal, ...args);

    yield* listener.iterator({ signal, ...options });
  }

  /**
   * Invokes the provided callback for each value emitted by a new resource established from this function.
   *
   * The established resource is disposed of when the callback throws, when the provided `signal` aborts,
   * or when the resource closes.
   *
   * Values emitted while the callback is still running are dropped according to the `queueingPolicy`,
   * which defaults to `EdgeQueuingPolicy`. Provide a `queueingPolicy` to buffer them instead.
   *
   * @param options - An `AbortSignal`, or the iteration options.
   * @param callback - A function invoked for each emitted value. May be asynchronous.
   * @param args - The arguments passed to the function.
   * @returns A promise which rejects when the iteration ends: with the callback's error when it throws, with the close reason when the resource closes, or with the abort reason when the signal aborts. It never resolves on its own: only a consumer-side termination ends the iteration cleanly, which `forEach` cannot trigger.
   *
   * @example
   * ```ts
   * await source.forEach(controller.signal, (value: number): void => console.log(value));
   * ```
   */
  async forEach(
    options: AbortSignal | ObservableAsyncFunctionToIteratorOptions,
    callback: (value: GValue) => PromiseLike<void> | void,
    ...args: GArguments
  ): Promise<void> {
    for await (const value of this.values(options, ...args)) {
      await callback(value);
    }
  }
}
