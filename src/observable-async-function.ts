import { AsyncFunction } from '@xstd/async-function';
import { EQUAL_FUNCTION_STRICT_EQUAL, type EqualFunction } from '@xstd/equal-function';
import {
  type EmitValue,
  ListenerResource,
  type ListenerResourceTOIteratorOptions,
} from '@xstd/listener-resource';
import type { MapFunction, MapFunctionArguments } from '@xstd/map-function';
import { NONE, type None } from '@xstd/none';
import type { OnCloseResource } from '@xstd/resource';

interface ObservableAsyncFunctionTransformOutputFunction<
  GIn,
  GOut,
  GArguments extends readonly unknown[],
> {
  (value: GIn, emit: EmitValue<GOut>, args: GArguments): void;
}

export interface ObservableAsyncFunctionMapFilterFunction<
  GIn,
  GOut,
  GArguments extends readonly unknown[],
> {
  (value: GIn, ...args: GArguments): GOut | None;
}

export interface ObservableAsyncFunctionFilterFunction<
  GValue,
  GArguments extends readonly unknown[],
> {
  (value: GValue, ...args: GArguments): boolean;
}

export interface ObservableAsyncFunctionFilterFunctionWithSubType<
  GIn,
  GOut extends GIn,
  GArguments extends readonly unknown[],
> {
  (value: GIn, ...args: GArguments): value is GOut;
}

export interface ObservableAsyncFunctionToIteratorOptions extends ListenerResourceTOIteratorOptions {
  readonly signal: AbortSignal;
}

export class ObservableAsyncFunction<
  GValue,
  GArguments extends readonly unknown[] = [],
> extends AsyncFunction<GArguments, ListenerResource<GValue>> {
  static merge<GValue, GArguments extends readonly unknown[]>(
    functions: ObservableAsyncFunction<GValue, GArguments>[],
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

  override mapArguments<GNewArguments extends readonly unknown[]>(
    mapFnc: MapFunctionArguments<GNewArguments, GArguments>,
  ): ObservableAsyncFunction<GValue, GNewArguments> {
    return new ObservableAsyncFunction<GValue, GNewArguments>(super.mapArguments(mapFnc));
  }

  /* ListenerResource  */

  #transformOutput<GNewValue>(
    transform: ObservableAsyncFunctionTransformOutputFunction<GValue, GNewValue, GArguments>,
  ): ObservableAsyncFunction<GNewValue, GArguments> {
    return new ObservableAsyncFunction<GNewValue, GArguments>(
      async (signal: AbortSignal, ...args: GArguments): Promise<ListenerResource<GNewValue>> => {
        const listener: ListenerResource<GValue> = await this.call(signal, ...args);

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

  map<GNewValue>(
    map: MapFunction<GValue, GNewValue>,
  ): ObservableAsyncFunction<GNewValue, GArguments> {
    return this.#transformOutput(
      (value: GValue, emit: EmitValue<GNewValue>): void => {
        emit(map(value));
      },
    );
  }

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

  filter<GNewValue extends GValue>(
    filter: ObservableAsyncFunctionFilterFunctionWithSubType<GValue, GNewValue, GArguments>,
  ): ObservableAsyncFunction<GNewValue, GArguments>;
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

  distinct(
    equal: EqualFunction<GValue> = EQUAL_FUNCTION_STRICT_EQUAL,
  ): ObservableAsyncFunction<GValue, GArguments> {
    return new ObservableAsyncFunction<GValue, GArguments>(
      async (signal: AbortSignal, ...args: GArguments): Promise<ListenerResource<GValue>> => {
        const listener: ListenerResource<GValue> = await this.call(signal, ...args);

        let lastValue: GValue | None = NONE;

        return new ListenerResource<GValue>(
          (emit: EmitValue<GValue>): OnCloseResource | void => {
            listener.listen((value: GValue): void => {
              if (lastValue === NONE || !equal(value, lastValue)) {
                lastValue = value;
                emit(value);
              }
            });

            return listener.close.bind(listener);
          },
        ).closesWith(listener);
      },
    );
  }

  async first(signal: AbortSignal, ...args: GArguments): Promise<GValue> {
    await using listener: ListenerResource<GValue> = await this.call(signal, ...args);

    return await listener.next({ signal });
  }

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
