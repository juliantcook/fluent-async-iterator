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
}
