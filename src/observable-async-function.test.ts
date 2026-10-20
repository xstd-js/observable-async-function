import { ListenerResource, type EmitValue } from '@xstd/listener-resource';
import { NONE, type None } from '@xstd/none';
import { describe, expect, test } from 'vitest';
import { ObservableAsyncFunction } from './observable-async-function.ts';

/* Tools  */

function sleep(duration: number, signal?: AbortSignal): Promise<void> {
  return new Promise<void>((resolve: () => void, reject: (reason?: unknown) => void): void => {
    signal?.throwIfAborted();

    const end = (): void => {
      signal?.removeEventListener('abort', onAbort);
      clearTimeout(timer);
    };

    const onAbort = (): void => {
      end();
      reject(signal!.reason);
    };

    signal?.addEventListener('abort', onAbort);

    const timer: ReturnType<typeof setTimeout> = setTimeout((): void => {
      end();
      resolve();
    }, duration);
  });
}

async function flush(ticks: number = 5): Promise<void> {
  for (let i = 0; i < ticks; i++) {
    await sleep(0);
  }
}

class TestSource {
  readonly emitters: Set<EmitValue<number>> = new Set<EmitValue<number>>();
  closeCount: number = 0;
  readonly fnc: ObservableAsyncFunction<number>;

  constructor() {
    this.fnc = new ObservableAsyncFunction<number>(async (): Promise<ListenerResource<number>> => {
      return new ListenerResource<number>((emit: EmitValue<number>): (() => void) => {
        this.emitters.add(emit);

        return (): void => {
          this.emitters.delete(emit);
          this.closeCount++;
        };
      });
    });
  }

  emit(...values: number[]): void {
    for (const emit of this.emitters) {
      for (const value of values) {
        emit(value);
      }
    }
  }
}

type OnCloseTeardown = () => void;

function mapToStringResource(listener: ListenerResource<number>): ListenerResource<string> {
  return new ListenerResource<string>((emit: EmitValue<string>): OnCloseTeardown => {
    listener.listen((value: number): void => {
      emit(String(value));
    });

    return listener.close.bind(listener);
  }).closesWith(listener);
}

/* Tests  */

describe('ObservableAsyncFunction', () => {
  describe('constructor', () => {
    test('wraps a call function', async () => {
      const source: TestSource = new TestSource();
      const fnc: ObservableAsyncFunction<number> = new ObservableAsyncFunction<number>(
        async (signal: AbortSignal): Promise<ListenerResource<number>> => {
          return source.fnc.call(signal);
        },
      );

      const controller: AbortController = new AbortController();
      const listener: ListenerResource<number> = await fnc.call(controller.signal);
      const nextPromise: Promise<number> = listener.next({ signal: controller.signal });
      await flush();

      source.emit(11);
      await flush();

      expect(await nextPromise).toBe(11);

      await listener.close();
    });

    test('reuses the underlying call of an existing AsyncFunction instance', async () => {
      const source: TestSource = new TestSource();
      const fnc: ObservableAsyncFunction<number> = new ObservableAsyncFunction<number>(source.fnc);

      const controller: AbortController = new AbortController();
      const listener: ListenerResource<number> = await fnc.call(controller.signal);
      const nextPromise: Promise<number> = listener.next({ signal: controller.signal });
      await flush();

      source.emit(12);
      await flush();

      expect(await nextPromise).toBe(12);

      await listener.close();
    });
  });

  describe('static methods', () => {
    describe('merge', () => {
      test('fans-in the values of all the sources', async () => {
        const sourceA: TestSource = new TestSource();
        const sourceB: TestSource = new TestSource();
        const merged: ObservableAsyncFunction<number> = ObservableAsyncFunction.merge([
          sourceA.fnc,
          sourceB.fnc,
        ]);

        const controller: AbortController = new AbortController();
        const listener: ListenerResource<number> = await merged.call(controller.signal);
        await flush();
        expect(sourceA.emitters.size).toBe(1);
        expect(sourceB.emitters.size).toBe(1);

        const received: number[] = [];
        listener.listen((value: number): void => {
          received.push(value);
        });

        sourceA.emit(1);
        sourceB.emit(2);
        sourceA.emit(3);
        await flush();

        expect(received).toEqual([1, 2, 3]);

        await listener.close();
        expect(sourceA.closeCount).toBe(1);
        expect(sourceB.closeCount).toBe(1);
      });

      test('closes when all the sources close', async () => {
        const emitters: EmitValue<number>[] = [];
        const controller: AbortController = new AbortController();
        const source: ObservableAsyncFunction<number> = new ObservableAsyncFunction<number>(
          async (): Promise<ListenerResource<number>> => {
            return new ListenerResource<number>((emit: EmitValue<number>): void => {
              emitters.push(emit);
            }).closesWith(controller.signal);
          },
        );

        const merged: ObservableAsyncFunction<number> = ObservableAsyncFunction.merge([source]);
        const listener: ListenerResource<number> = await merged.call(new AbortController().signal);
        await flush();
        expect(listener.throwIfClosed()).toBeUndefined();

        controller.abort();
        await flush();
        expect(listener.closeSignal.aborted).toBe(true);
      });

      test('closes the sources opened so far when one source fails', async () => {
        let aClosed: boolean = false;
        const a: ObservableAsyncFunction<number> = new ObservableAsyncFunction<number>(
          async (): Promise<ListenerResource<number>> => {
            return new ListenerResource<number>((): (() => void) => {
              return (): void => {
                aClosed = true;
              };
            });
          },
        );
        const b: ObservableAsyncFunction<number> = new ObservableAsyncFunction<number>(
          async (): Promise<ListenerResource<number>> => {
            throw new Error('boom');
          },
        );

        const merged: ObservableAsyncFunction<number> = ObservableAsyncFunction.merge([a, b]);
        await expect(merged.call(new AbortController().signal)).rejects.toThrow('boom');
        await flush();

        expect(aClosed).toBe(true);
      });

      test('rejects and closes the sources opened so far when one source resolves closed', async () => {
        let aClosed: boolean = false;
        const a: ObservableAsyncFunction<number> = new ObservableAsyncFunction<number>(
          async (): Promise<ListenerResource<number>> => {
            return new ListenerResource<number>((): (() => void) => {
              return (): void => {
                aClosed = true;
              };
            });
          },
        );
        const closed: ObservableAsyncFunction<number> = new ObservableAsyncFunction<number>(
          async (): Promise<ListenerResource<number>> => {
            const resource: ListenerResource<number> = new ListenerResource<number>((): void => {});
            await resource.close();
            return resource;
          },
        );

        const merged: ObservableAsyncFunction<number> = ObservableAsyncFunction.merge([a, closed]);
        await expect(merged.call(new AbortController().signal)).rejects.toThrow();
        await flush();

        expect(aClosed).toBe(true);
      });

      test('rejects when the call signal aborts during the setup', async () => {
        const slow: ObservableAsyncFunction<number> = new ObservableAsyncFunction<number>(
          async (signal: AbortSignal): Promise<ListenerResource<number>> => {
            await sleep(1000, signal);
            return new ListenerResource<number>((): void => {});
          },
        );

        const merged: ObservableAsyncFunction<number> = ObservableAsyncFunction.merge([slow]);
        const controller: AbortController = new AbortController();
        const callPromise: Promise<ListenerResource<number>> = merged.call(controller.signal);
        await flush();
        controller.abort();

        await expect(callPromise).rejects.toThrow();
      });

      test('with no source, emits nothing and never closes on its own', async () => {
        const merged: ObservableAsyncFunction<number> = ObservableAsyncFunction.merge([]);

        const controller: AbortController = new AbortController();
        const listener: ListenerResource<number> = await merged.call(controller.signal);
        await flush();

        expect(listener.throwIfClosed()).toBeUndefined(); // still open

        const received: number[] = [];
        listener.listen((value: number): void => {
          received.push(value);
        });
        await flush();
        expect(received).toEqual([]);

        await listener.close(); // the consumer is responsible for closing it
      });
    });
  });

  describe('methods', () => {
    describe('mapArguments', () => {
      test('maps the arguments before calling', async () => {
        const receivedArgs: string[] = [];
        const source: ObservableAsyncFunction<number, [string]> = new ObservableAsyncFunction<
          number,
          [string]
        >(async (_signal: AbortSignal, id: string): Promise<ListenerResource<number>> => {
          receivedArgs.push(id);
          return new ListenerResource<number>((emit: EmitValue<number>): void => {
            setTimeout((): void => emit(id.length), 0); // must emit after establishment
          });
        });

        const mapped: ObservableAsyncFunction<number, [number]> = source.mapArguments(
          (index: number): [string] => [`id-${index}`],
        );

        const controller: AbortController = new AbortController();
        const listener: ListenerResource<number> = await mapped.call(controller.signal, 42);
        const nextPromise: Promise<number> = listener.next();
        await flush();

        expect(await nextPromise).toBe(5);
        expect(receivedArgs).toEqual(['id-42']);

        await listener.close();
      });
    });

    describe('then', () => {
      test('without handlers, returns a function with the same behaviour', async () => {
        const source: TestSource = new TestSource();
        const cloned: ObservableAsyncFunction<number> = source.fnc.then();

        const controller: AbortController = new AbortController();
        const listener: ListenerResource<number> = await cloned.call(controller.signal);
        const nextPromise: Promise<number> = listener.next({ signal: controller.signal });
        await flush();

        source.emit(7);
        await flush();

        expect(await nextPromise).toBe(7);

        await listener.close();
      });

      test('transforms the resolved resource', async () => {
        const source: TestSource = new TestSource();
        const transformed: ObservableAsyncFunction<string> = source.fnc.then(mapToStringResource);

        const controller: AbortController = new AbortController();
        const listener: ListenerResource<string> = await transformed.call(controller.signal);
        const nextPromise: Promise<string> = listener.next({ signal: controller.signal });

        source.emit(42);
        await flush();

        expect(await nextPromise).toBe('42');

        await listener.close();
        expect(source.closeCount).toBe(1); // closing the transformed resource cascades upstream
      });

      test('rejects when the call fails without a rejection handler', async () => {
        const failing: ObservableAsyncFunction<number> = new ObservableAsyncFunction<number>(
          async (): Promise<ListenerResource<number>> => {
            throw new Error('boom');
          },
        );

        await expect(failing.then().call(new AbortController().signal)).rejects.toThrow('boom');
      });

      test('with a rejection handler only, resolves with the fallback resource', async () => {
        const failing: ObservableAsyncFunction<number> = new ObservableAsyncFunction<number>(
          async (): Promise<ListenerResource<number>> => {
            throw new Error('boom');
          },
        );

        const recovered: ObservableAsyncFunction<number | string> = failing.then(
          undefined,
          (): ListenerResource<number | string> =>
            new ListenerResource<number | string>((emit: EmitValue<number | string>): void => {
              setTimeout((): void => emit('fallback'), 0); // must emit after establishment
            }),
        );

        const controller: AbortController = new AbortController();
        const listener: ListenerResource<number | string> = await recovered.call(controller.signal);
        expect(await listener.next()).toBe('fallback');

        await listener.close();
      });
    });

    describe('catch', () => {
      test('without a handler, returns a function with the same behaviour', async () => {
        const source: TestSource = new TestSource();
        const cloned: ObservableAsyncFunction<number> = source.fnc.catch();

        const controller: AbortController = new AbortController();
        const listener: ListenerResource<number> = await cloned.call(controller.signal);
        const nextPromise: Promise<number> = listener.next({ signal: controller.signal });

        source.emit(3);
        await flush();

        expect(await nextPromise).toBe(3);

        await listener.close();
      });

      test('resolves with the fallback resource when the call fails', async () => {
        const failing: ObservableAsyncFunction<number> = new ObservableAsyncFunction<number>(
          async (): Promise<ListenerResource<number>> => {
            throw new Error('boom');
          },
        );

        const recovered: ObservableAsyncFunction<number | string> = failing.catch(
          (): ListenerResource<number | string> =>
            new ListenerResource<number | string>((emit: EmitValue<number | string>): void => {
              setTimeout((): void => emit('fallback'), 0); // must emit after establishment
            }),
        );

        const controller: AbortController = new AbortController();
        const listener: ListenerResource<number | string> = await recovered.call(controller.signal);
        expect(await listener.next()).toBe('fallback');

        await listener.close();
      });
    });

    describe('shareConcurentCalls', () => {
      test('shares concurrent calls with the same key', async () => {
        let callCount: number = 0;
        const source: ObservableAsyncFunction<number> = new ObservableAsyncFunction<number>(
          async (): Promise<ListenerResource<number>> => {
            callCount++;
            await sleep(20);
            return new ListenerResource<number>((emit: EmitValue<number>): void => {
              setTimeout((): void => emit(callCount), 0); // must emit after establishment
            });
          },
        );

        const shared: ObservableAsyncFunction<number> = source.shareConcurentCalls({
          keyGenerator: (): string => 'key',
        });

        const controller: AbortController = new AbortController();
        const [listenerA, listenerB]: ListenerResource<number>[] = await Promise.all([
          shared.call(controller.signal),
          shared.call(controller.signal),
        ]);

        expect(callCount).toBe(1);
        const [valueA, valueB]: number[] = await Promise.all([listenerA.next(), listenerB.next()]);
        expect(valueA).toBe(1);
        expect(valueB).toBe(1);
      });
    });

    describe('map', () => {
      test('emits the mapped values', async () => {
        const source: TestSource = new TestSource();
        const mapped: ObservableAsyncFunction<string> = source.fnc.map(
          (value: number): string => `v${value}`,
        );

        const controller: AbortController = new AbortController();
        const listener: ListenerResource<string> = await mapped.call(controller.signal);
        const nextPromise: Promise<string> = listener.next({ signal: controller.signal });

        source.emit(4);
        await flush();

        expect(await nextPromise).toBe('v4');

        await listener.close();
        expect(source.closeCount).toBe(1);
      });

      test('rejects when the upstream resolves closed', async () => {
        const closed: ObservableAsyncFunction<number> = new ObservableAsyncFunction<number>(
          async (): Promise<ListenerResource<number>> => {
            const resource: ListenerResource<number> = new ListenerResource<number>((): void => {});
            await resource.close();
            return resource;
          },
        );

        const mapped: ObservableAsyncFunction<string> = closed.map(
          (value: number): string => `v${value}`,
        );

        await expect(mapped.call(new AbortController().signal)).rejects.toThrow();
      });
    });

    describe('mapFilter', () => {
      test('emits the mapped values and drops the NONE ones', async () => {
        const source: ObservableAsyncFunction<number, [string]> = new ObservableAsyncFunction<
          number,
          [string]
        >(async (): Promise<ListenerResource<number>> => {
          return new ListenerResource<number>((emit: EmitValue<number>): void => {
            setTimeout((): void => {
              emit(1);
              emit(2);
              emit(3);
            }, 0); // must emit after establishment
          });
        });

        const mapped: ObservableAsyncFunction<string, [string]> = source.mapFilter(
          (value: number, prefix: string): string | None =>
            value % 2 === 1 ? `${prefix}${value}` : NONE,
        );

        const controller: AbortController = new AbortController();
        const listener: ListenerResource<string> = await mapped.call(controller.signal, 'v-');

        const received: string[] = [];
        listener.listen((value: string): void => {
          received.push(value);
        });
        await flush();

        expect(received).toEqual(['v-1', 'v-3']);

        await listener.close();
      });
    });

    describe('filter', () => {
      test('emits the values satisfying the predicate', async () => {
        const source: TestSource = new TestSource();
        const filtered: ObservableAsyncFunction<number> = source.fnc.filter(
          (value: number): boolean => value % 2 === 0,
        );

        const controller: AbortController = new AbortController();
        const listener: ListenerResource<number> = await filtered.call(controller.signal);

        const received: number[] = [];
        listener.listen((value: number): void => {
          received.push(value);
        });

        source.emit(1, 2, 3, 4);
        await flush();

        expect(received).toEqual([2, 4]);

        await listener.close();
      });

      test('narrows the emitted values with a type-guard predicate', async () => {
        const source: ObservableAsyncFunction<number | string> = new ObservableAsyncFunction<
          number | string
        >(async (): Promise<ListenerResource<number | string>> => {
          return new ListenerResource<number | string>((emit: EmitValue<number | string>): void => {
            setTimeout((): void => {
              emit(1);
              emit('a');
              emit(2);
              emit('b');
            }, 0); // must emit after establishment
          });
        });

        const strings: ObservableAsyncFunction<string> = source.filter(
          (value: number | string): value is string => typeof value === 'string',
        );

        const controller: AbortController = new AbortController();
        const listener: ListenerResource<string> = await strings.call(controller.signal);

        const received: string[] = [];
        listener.listen((value: string): void => {
          received.push(value);
        });
        await flush();

        expect(received).toEqual(['a', 'b']);

        await listener.close();
      });

      test('forwards the call arguments to the predicate', async () => {
        const source: ObservableAsyncFunction<number, [number]> = new ObservableAsyncFunction<
          number,
          [number]
        >(async (): Promise<ListenerResource<number>> => {
          return new ListenerResource<number>((emit: EmitValue<number>): void => {
            setTimeout((): void => {
              emit(1);
              emit(10);
              emit(100);
            }, 0); // must emit after establishment
          });
        });

        const filtered: ObservableAsyncFunction<number, [number]> = source.filter(
          (value: number, min: number): boolean => value > min,
        );

        const controller: AbortController = new AbortController();
        const listener: ListenerResource<number> = await filtered.call(controller.signal, 5);

        const received: number[] = [];
        listener.listen((value: number): void => {
          received.push(value);
        });
        await flush();

        expect(received).toEqual([10, 100]);

        await listener.close();
      });
    });

    describe('distinct', () => {
      test('filters out the consecutive duplicates', async () => {
        const source: TestSource = new TestSource();
        const distinct: ObservableAsyncFunction<number> = source.fnc.distinct();

        const controller: AbortController = new AbortController();
        const listener: ListenerResource<number> = await distinct.call(controller.signal);

        const received: number[] = [];
        listener.listen((value: number): void => {
          received.push(value);
        });

        source.emit(1, 1, 2, 2, 1);
        await flush();

        expect(received).toEqual([1, 2, 1]);

        await listener.close();
      });

      test('accepts a custom equality function', async () => {
        const source: TestSource = new TestSource();
        const distinct: ObservableAsyncFunction<number> = source.fnc.distinct(
          (a: number, b: number): boolean => Math.floor(a) === Math.floor(b),
        );

        const controller: AbortController = new AbortController();
        const listener: ListenerResource<number> = await distinct.call(controller.signal);

        const received: number[] = [];
        listener.listen((value: number): void => {
          received.push(value);
        });

        source.emit(1.1, 1.9, 2.2);
        await flush();

        expect(received).toEqual([1.1, 2.2]);

        await listener.close();
      });

      test('keeps the deduplication state per subscription', async () => {
        const source: TestSource = new TestSource();
        const distinct: ObservableAsyncFunction<number> = source.fnc.distinct();

        // subscription A receives 5
        const controllerA: AbortController = new AbortController();
        const listenerA: ListenerResource<number> = await distinct.call(controllerA.signal);
        const nextPromiseA: Promise<number> = listenerA.next({ signal: controllerA.signal });
        await flush();
        source.emit(5);
        await flush();
        expect(await nextPromiseA).toBe(5);

        // subscription B starts after: its first value must not be filtered out
        const controllerB: AbortController = new AbortController();
        const listenerB: ListenerResource<number> = await distinct.call(controllerB.signal);
        const nextPromiseB: Promise<number> = listenerB.next({ signal: controllerB.signal });
        await flush();
        source.emit(5);
        await flush();

        expect(await nextPromiseB).toBe(5);

        await listenerA.close();
        await listenerB.close();
      });
    });

    describe('first', () => {
      test('resolves with the first emitted value and disposes of the resource', async () => {
        const source: TestSource = new TestSource();
        const firstPromise: Promise<number> = source.fnc.first(new AbortController().signal);
        await flush();

        source.emit(9);
        await flush();

        expect(await firstPromise).toBe(9);
        expect(source.closeCount).toBe(1); // disposed after the first value
      });

      test('rejects when the signal aborts before any value', async () => {
        const source: TestSource = new TestSource();
        const controller: AbortController = new AbortController();
        const firstPromise: Promise<number> = source.fnc.first(controller.signal);
        await flush();

        controller.abort();

        await expect(firstPromise).rejects.toThrow();
        expect(source.closeCount).toBe(1);
      });

      test('rejects when the resource closes before any value', async () => {
        const source: ObservableAsyncFunction<number> = new ObservableAsyncFunction<number>(
          async (): Promise<ListenerResource<number>> => {
            const resource: ListenerResource<number> = new ListenerResource<number>((): void => {});
            await resource.close();
            return resource;
          },
        );

        await expect(source.first(new AbortController().signal)).rejects.toThrow();
      });
    });

    describe('values', () => {
      test('yields the emitted values', async () => {
        const source: TestSource = new TestSource();
        const controller: AbortController = new AbortController();
        const iterator: AsyncGenerator<number> = source.fnc.values(controller.signal);
        const nextPromise: Promise<IteratorResult<number>> = iterator.next();
        await flush();

        source.emit(1);
        await flush();

        expect(await nextPromise).toEqual({ done: false, value: 1 });

        await iterator.return?.(undefined);
        expect(source.closeCount).toBe(1); // disposed when the iteration ends
      });

      test('disposes of the resource on break', async () => {
        const source: TestSource = new TestSource();

        const received: number[] = [];
        const consuming: Promise<void> = (async (): Promise<void> => {
          for await (const value of source.fnc.values(new AbortController().signal)) {
            received.push(value);
            if (value === 2) {
              break;
            }
          }
        })();

        await flush();
        source.emit(1);
        await flush();
        source.emit(2);
        await flush();
        await consuming;

        expect(received).toEqual([1, 2]);
        expect(source.closeCount).toBe(1); // disposed when the iteration ends
      });

      test('rejects when the signal aborts', async () => {
        const source: TestSource = new TestSource();
        const controller: AbortController = new AbortController();
        const iterator: AsyncGenerator<number> = source.fnc.values(controller.signal);
        const nextPromise: Promise<IteratorResult<number>> = iterator.next();
        await flush();

        controller.abort();

        await expect(nextPromise).rejects.toThrow();
      });

      test('accepts an options object', async () => {
        const source: TestSource = new TestSource();
        const controller: AbortController = new AbortController();
        const iterator: AsyncGenerator<number> = source.fnc.values({ signal: controller.signal });
        const nextPromise: Promise<IteratorResult<number>> = iterator.next();
        await flush();

        source.emit(6);
        await flush();

        expect(await nextPromise).toEqual({ done: false, value: 6 });

        await iterator.return?.(undefined);
      });
    });

    describe('forEach', () => {
      test('invokes the callback for each emitted value', async () => {
        const emitters: EmitValue<number>[] = [];
        const sourceController: AbortController = new AbortController();
        const source: ObservableAsyncFunction<number> = new ObservableAsyncFunction<number>(
          async (): Promise<ListenerResource<number>> => {
            return new ListenerResource<number>((emit: EmitValue<number>): void => {
              emitters.push(emit);
            }).closesWith(sourceController.signal);
          },
        );

        const received: number[] = [];
        const consuming: Promise<void> = source.forEach(
          new AbortController().signal,
          (value: number): void => {
            received.push(value);
          },
        );
        const assertion: Promise<void> = expect(consuming).rejects.toThrow(); // ends with the stream's closure

        await flush();
        for (const emit of emitters) {
          emit(1);
        }
        await flush();
        for (const emit of emitters) {
          emit(2);
        }
        await flush();
        sourceController.abort(); // closes the stream, ending the forEach
        await assertion;

        expect(received).toEqual([1, 2]);
      });

      test('rejects and disposes of the resource when the callback throws', async () => {
        const source: TestSource = new TestSource();
        const controller: AbortController = new AbortController();
        const forEachPromise: Promise<void> = source.fnc.forEach(
          controller.signal,
          (value: number): void => {
            if (value === 2) {
              throw new Error('callback error');
            }
          },
        );
        const assertion: Promise<void> = expect(forEachPromise).rejects.toThrow('callback error');
        await flush();

        source.emit(1);
        await flush();
        source.emit(2);
        await flush();

        await assertion;
        expect(source.closeCount).toBe(1);
      });
    });
  });
});
