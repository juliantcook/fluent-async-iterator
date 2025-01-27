interface GroupedByKey<T> {
    key: string;
    group: T[];
}

/**
 * The items must already be sorted by their key.
 */
export async function* groupIterator<T>(sortedSource: AsyncIterable<T>, groupBy: string): AsyncIterable<GroupedByKey<T>> {
    let currentGroup: GroupedByKey<T> | undefined;
    const setCurrentGroup = item => currentGroup = { key: item[groupBy], group: [item] };
    for await (const item of sortedSource) {
        if (!currentGroup) {
            setCurrentGroup(item);
        } else if (currentGroup.key == item[groupBy]) {
            currentGroup.group.push(item);
        } else {
            yield currentGroup;
            setCurrentGroup(item);
        }
    }
    if (currentGroup) {
        yield currentGroup;
    }
}

export async function* asyncMapIterator<T, U>(source: AsyncIterable<T>, func: (i: T) => U): AsyncIterable<U> {
    for await (const item of source) {
        yield func(item);
    }
}

export function* mapIterator<T, U>(source: Iterable<T>, func: (i: T) => U): Iterable<U> {
    for (const item of source) {
        yield func(item);
    }
}

export async function* batchIterator<T>(source: AsyncIterable<T>, size: number): AsyncIterable<T[]> {
    let batch: T[] = [];
    for await (const item of source) {
        batch.push(item);
        if (batch.length === size) {
            yield batch;
            batch = [];
        }
    }
    if (batch.length > 0) {
        yield batch;
    }
}

export async function* filterIterator<T>(source: AsyncIterable<T>, predicate: (p: T) => boolean): AsyncIterable<T> {
    for await (const item of source) {
        if (predicate(item)) {
            yield item;
        }
    }
}

export async function* intervalIterator<T>(source: AsyncIterable<T>, ms: number): AsyncIterable<T> {
    for await (const item of source) {
        let deltaStart = Date.now();
        yield item;
        let delta = Date.now() - deltaStart;
        await delay(ms - delta);
    }
}

export async function* limitIterator<T>(source: AsyncIterable<T>, count: number): AsyncIterable<T> {
    const gen = source as AsyncGenerator<T>;
    let i = 0;
    while (++i <= count) {
        const { value, done } = await gen.next();
        if (!done) {
            yield value;
        } else {
            break;
        }
    }
}

export async function* peekIterator<T>(source: AsyncIterable<T>, func: (i: T) => any): AsyncIterable<T> {
    for await (const item of source) {
        func(item);
        yield item;
    }
}

export async function* splitIterator<T>(source: AsyncIterable<T>, predicate: (i: T) => boolean): AsyncIterable<T[]> {
    let currentSplit: T[] = [];
    for await (const item of source) {
        if (predicate(item)) {
            yield currentSplit;
            currentSplit = [];
        } else {
            currentSplit.push(item);
        }
    }
    if (currentSplit.length > 0) {
        yield currentSplit;
    }
}

type ConcurrencyPool<U> = { [slot: number]: Promise<{ slot: number, result: U }> }

export async function* concurrentMapIterator<T, U>(source: AsyncIterable<T>, func: (i: T) => Promise<U>, count: number): AsyncIterable<U> {
    const pool: ConcurrencyPool<U> = {};
    const slots = [...Array(count).keys()]
    if (slots.length < 1) {
        throw new Error('Invalid concurrency count')
    }
    for await (const item of source) {
        const slot = slots.pop()!;
        pool[slot] = func(item).then(result => ({slot, result}));
        if (slots.length < 1) {
            yield Promise.race(Object.values(pool)).then((completed) => {
                slots.push(completed.slot)
                return completed.result;
            });
        }
    }
    yield * racingDrain(pool);
}

export async function* concurrentIterator<T>(source: Iterable<Promise<T>>, count: number): AsyncIterable<T> {
    const pool: ConcurrencyPool<T> = {};
    const slots = [...Array(count).keys()]
    if (slots.length < 1) {
        throw new Error('Invalid concurrency count')
    }
    for (const promise of source) {
        const slot = slots.pop()!;
        pool[slot] = promise.then(result => ({slot, result}));
        if (slots.length < 1) {
            yield Promise.race(Object.values(pool)).then((completed) => {
                slots.push(completed.slot)
                return completed.result;
            });
        }
    }
    yield * racingDrain(pool);
}

async function* racingDrain<T>(pool: ConcurrencyPool<T>): AsyncIterable<T> {
    let promises = Object.values(pool)
    while (promises.length > 0) {
        const completed = await Promise.race(promises);
        yield completed.result;
        delete pool[completed.slot]
        promises = Object.values(pool)
    }
}

function delay(ms: number): Promise<void> {
    return new Promise(resolve => setTimeout(resolve, ms));
}

export async function * toAsyncIterable<T>(source: Iterable<Promise<T>>): AsyncIterable<T> {
    for (const item of source) {
        yield item
    }
}

export function iterator<T>(source: AsyncIterable<T>): FluentAsyncIterator<T> {
    return new FluentAsyncIterator(source);
}

export function promiseIterator<T>(source: Iterable<Promise<T>>): FluentPromiseIterator<T> {
    return new FluentPromiseIterator(source);
}

export class FluentAsyncIterator<T> {
    constructor(private source: AsyncIterable<T>) { }

    async collect(): Promise<T[]> {
        const results: T[] = [];
        for await (const item of this.source) {
            results.push(item);
        }
        return results;
    }

    async drain(): Promise<void> {
        for await (const _item of this.source) {
            // do nothing
        }
    }

    iterable(): AsyncIterable<T> {
        return this.source;
    }

    batch(size: number): FluentAsyncIterator<T[]> {
        return new FluentAsyncIterator(batchIterator(this.source, size));
    }

    group(key: string): FluentAsyncIterator<GroupedByKey<T>> {
        return new FluentAsyncIterator(groupIterator(this.source, key));
    }

    map<U>(func: (i: T) => U): FluentAsyncIterator<U> {
        return new FluentAsyncIterator(asyncMapIterator(this.source, func));
    }

    filter(predicate: (p: T) => boolean): FluentAsyncIterator<T> {
        return new FluentAsyncIterator(filterIterator(this.source, predicate));
    }

    interval(ms: number): FluentAsyncIterator<T> {
        return new FluentAsyncIterator(intervalIterator(this.source, ms));
    }

    limit(count: number): FluentAsyncIterator<T> {
        return new FluentAsyncIterator(limitIterator(this.source, count));
    }

    peek(func: (i: T) => any): FluentAsyncIterator<T> {
        return new FluentAsyncIterator(peekIterator(this.source, func));
    }

    split(predicate: (i: T) => boolean): FluentAsyncIterator<T[]> {
        return new FluentAsyncIterator(splitIterator(this.source, predicate));
    }

    /**
     * @param func - function to apply
     * @param count - concurrent limit
     *
     * Does not maintain the order
     */
    concurrentMap<U>(func: (i: T) => Promise<U>, count: number): FluentAsyncIterator<U> {
        return new FluentAsyncIterator(concurrentMapIterator(this.source, func, count));
    }
}

export class FluentPromiseIterator<T> {
    constructor(private source: Iterable<Promise<T>>) { }

    concurrentResolve(count: number): FluentAsyncIterator<T> {
        return new FluentAsyncIterator<T>(concurrentIterator(this.source, count));
    }
    
    asyncIterator(): FluentAsyncIterator<T> {
        return new FluentAsyncIterator(toAsyncIterable(this.source));
    }
}
