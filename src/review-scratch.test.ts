import { ListenerResource, type EmitValue } from '@xstd/listener-resource';
import { expect, test } from 'vitest';
import { ObservableAsyncFunction } from './observable-async-function.ts';

const flush = async (ticks = 5): Promise<void> => {
  for (let i = 0; i < ticks; i++) {
    await new Promise<void>((resolve) => setTimeout(resolve, 0));
  }
};

test('A: distinct state must NOT be shared between subscriptions', async () => {
  const emitters = new Set<EmitValue<number>>();
  const source = new ObservableAsyncFunction<number>(
    async (): Promise<ListenerResource<number>> => {
      return new ListenerResource<number>((emit: EmitValue<number>): void => {
        emitters.add(emit);
      });
    },
  );
  const distinct = source.distinct();

  // subscriber A
  const controllerA: AbortController = new AbortController();
  const iteratorA = distinct.values(controllerA.signal);
  const promiseA = iteratorA.next();
  await flush();

  // emit 5 -> A receives it
  for (const emit of emitters) {
    emit(5);
  }
  await flush();
  expect(await Promise.race([promiseA, Promise.resolve({ done: true as const })])).toEqual({
    done: false,
    value: 5,
  });

  // subscriber B, after A already saw 5
  const iteratorB = distinct.values(new AbortController().signal);
  const promiseB = iteratorB.next();
  await flush();

  // emit 5 again: B must receive it (first value for B), A must not
  for (const emit of emitters) {
    emit(5);
  }
  await flush();
  expect(await Promise.race([promiseB, Promise.resolve({ done: true as const })])).toEqual({
    done: false,
    value: 5,
  });

  controllerA.abort();
  await flush();
});

test('A2: distinct still dedups consecutive duplicates within one subscription', async () => {
  const emitters = new Set<EmitValue<number>>();
  const source = new ObservableAsyncFunction<number>(
    async (): Promise<ListenerResource<number>> => {
      return new ListenerResource<number>((emit: EmitValue<number>): void => {
        emitters.add(emit);
      });
    },
  );

  const received: number[] = [];
  const controller: AbortController = new AbortController();
  const iterator = source.distinct().values(controller.signal);
  const consuming = (async (): Promise<void> => {
    for await (const value of iterator) {
      received.push(value);
    }
  })();
  await flush();

  for (const emit of emitters) {
    emit(1);
    emit(1);
    emit(2);
    emit(2);
    emit(1);
  }
  await flush();
  controller.abort();
  await expect(consuming).rejects.toThrow();

  expect(received).toEqual([1, 2, 1]);
});

test('B: merge must close already-opened sources when another source fails', async () => {
  let aClosed = false;
  const a = new ObservableAsyncFunction<number>(
    async (): Promise<ListenerResource<number>> => {
      return new ListenerResource<number>((): (() => void) => {
        return (): void => {
          aClosed = true;
        };
      });
    },
  );
  const b = new ObservableAsyncFunction<number>(
    async (): Promise<ListenerResource<number>> => {
      throw new Error('boom');
    },
  );

  const merged = ObservableAsyncFunction.merge([a, b]);
  const controller: AbortController = new AbortController();
  await expect(merged.call(controller.signal)).rejects.toThrow('boom');
  await flush();

  expect(aClosed, 'source A must be closed after merge setup failure').toBe(true);
});

test('C: merge must not close signal-tying sources after successful setup', async () => {
  let cClosed = false;
  const c = new ObservableAsyncFunction<number>(
    async (signal: AbortSignal): Promise<ListenerResource<number>> => {
      return new ListenerResource<number>((_emit: EmitValue<number>): (() => void) => {
        return (): void => {
          cClosed = true;
        };
      }).closesWith(signal);
    },
  );

  const merged = ObservableAsyncFunction.merge([c]);
  const controller: AbortController = new AbortController();
  const listener = await merged.call(controller.signal);
  await flush();

  expect(listener.closeSignal.aborted, 'merged resource must stay open').toBe(false);
  expect(cClosed, 'source C must stay open').toBe(false);

  await listener.close();
  expect(cClosed, 'closing merged must cascade to source C').toBe(true);
});

test('D: merge fans-in values from all sources and cascades close to sources', async () => {
  const emitters = new Set<EmitValue<number>>();
  let closedCount = 0;
  const makeSource = (): ObservableAsyncFunction<number> => {
    return new ObservableAsyncFunction<number>(
      async (): Promise<ListenerResource<number>> => {
        return new ListenerResource<number>((emit: EmitValue<number>): (() => void) => {
          emitters.add(emit);
          return (): void => {
            emitters.delete(emit);
            closedCount++;
          };
        });
      },
    );
  };

  const merged = ObservableAsyncFunction.merge([makeSource(), makeSource()]);
  const controller: AbortController = new AbortController();
  const listener = await merged.call(controller.signal);
  await flush();
  expect(emitters.size).toBe(2);

  const nextPromise = listener.next({ signal: controller.signal });
  for (const emit of emitters) {
    emit(1);
  }
  await flush();
  expect(await nextPromise).toEqual(1);

  await listener.close();
  await flush();
  expect(closedCount, 'closing merged must cascade to both sources').toBe(2);
  expect(emitters.size).toBe(0);
});
