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

export async function* mapIterator<T, U>(source: AsyncIterable<T>, func: (i: T) => U): AsyncIterable<U> {
    for await (const item of source) {
        yield func(item);
    }
}

export async function* batchIterator<T>(source: AsyncIterable<T>, size: number): AsyncIterable<T[]> {
    let batch: T[] = [];
    for await (const item of source) {
        if (batch.length < size) {
            batch.push(item);
        } else {
            yield batch;
            batch = [item];
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

export async function* concurrentMapIterator<T, U>(source: AsyncIterable<T>, func: (i: T) => Promise<U>, count: number): AsyncIterable<U> {
    const pool: { [k: number]: Promise<{ slot: number, result: U }> } = {};
    let slot = 1;
    for await (const item of source) {
        if (slot <= count) {
            const _slot = slot++; // local reference
            pool[_slot] = func(item).then(result => ({ slot: _slot, result }));
        } else {
            yield Promise.race(Object.values(pool)).then((completed) => {
                pool[completed.slot] = func(item).then(result => ({ slot: completed.slot, result }));
                return completed.result;
            });
        }
    }
    yield* Object.values(pool)
        .map(promise => promise.then(({ result }) => result));
}

function delay(ms: number): Promise<void> {
    return new Promise(resolve => setTimeout(resolve, ms));
}

export function iterator<T>(source: AsyncIterable<T>): FluentAsyncIterator<T> {
    return new FluentAsyncIterator(source);
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
        return new FluentAsyncIterator(mapIterator(this.source, func));
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
