interface GroupedByKey<T> {
    key: string;
    group: T[];
}

/**
 * The items must already be sorted by their key.
 */
export async function* toGroupedGenerator<T>(sortedSource: AsyncGenerator<T>, groupBy: string): AsyncGenerator<GroupedByKey<T>> {
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

export async function* mapGenerator<T, U>(source: AsyncGenerator<T>, func: (i: T) => U): AsyncGenerator<U> {
    for await (const item of source) {
        yield func(item);
    }
}

export async function* batchGenerator<T>(source: AsyncGenerator<T>, size: number): AsyncGenerator<T[]> {
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

export async function* filterGenerator<T>(source: AsyncGenerator<T>, predicate: (p: T) => boolean): AsyncGenerator<T> {
    for await (const item of source) {
        if (predicate(item)) {
            yield item;
        }
    }
}

export class FluentAsyncGenerator<T> {
    constructor(private source: AsyncGenerator<T>) { }

    async collect(): Promise<T[]> {
        const results: T[] = [];
        for await (const item of this.source) {
            results.push(item);
        }
        return results;
    }

    generator(): AsyncGenerator<T> {
        return this.source;
    }

    batch(size: number): FluentAsyncGenerator<T[]> {
        return new FluentAsyncGenerator(batchGenerator(this.source, size));
    }

    group(key: string): FluentAsyncGenerator<GroupedByKey<T>> {
        return new FluentAsyncGenerator(toGroupedGenerator(this.source, key));
    }

    map<U>(func: (i: T) => U): FluentAsyncGenerator<U> {
        return new FluentAsyncGenerator(mapGenerator(this.source, func));
    }

    filter(predicate: (p: T) => boolean): FluentAsyncGenerator<T> {
        return new FluentAsyncGenerator(filterGenerator(this.source, predicate));
    }
}
