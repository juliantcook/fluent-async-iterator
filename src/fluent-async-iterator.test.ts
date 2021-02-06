import { strict as assert } from "assert";
import { groupIterator, batchIterator, FluentAsyncIterator, iterator } from "./fluent-async-iterator";

describe('groupIterator', () => {
    it('groups sorted objects by given key and returns them as they are iterated', async () => {
        async function* objectsSortedByKey() {
            yield* [
                { foo: '1', bar: 'a' },
                { foo: '1', bar: 'b' },
                { foo: '2', bar: 'c' },
                { foo: '3', bar: 'd' },
            ];
        }
        const groupedIterator = groupIterator(objectsSortedByKey(), 'foo');
        const results: any[] = [];
        for await (const group of groupedIterator) {
            results.push(group);
        }
        assert.deepEqual(results, [
            { key: '1', group: [{ foo: '1', bar: 'a' }, { foo: '1', bar: 'b' }] },
            { key: '2', group: [{ foo: '2', bar: 'c' }] },
            { key: '3', group: [{ foo: '3', bar: 'd' }] }
        ]);
    });

    it('handles empty iterator', async () => {
        async function* objectsSortedByKey() {
            yield* [];
        }
        const groupedIterator = groupIterator(objectsSortedByKey(), 'foo');
        const results: any[] = [];
        for await (const group of groupedIterator) {
            results.push(group);
        }
        assert.deepEqual(results, []);
    });
});

describe('batchIterator', () => {
    it('creates a iterator of batches', async () => {
        async function* source() {
            yield* [1, 2, 3, 4, 5];
        }
        const batched = batchIterator(source(), 2);
        const results: any[] = [];
        for await (const batch of batched) {
            results.push(batch);
        }
        assert.deepEqual(results, [
            [1, 2],
            [3, 4],
            [5]
        ]);
    });

    it('handles empty iterator', async () => {
        async function* source() {
            yield* [];
        }
        const batched = batchIterator(source(), 2);
        const results: any[] = [];
        for await (const batch of batched) {
            results.push(batch);
        }
        assert.deepEqual(results, []);
    });
});

describe('FluentAsyncIterator', () => {
    it('collects results', async () => {
        async function* source() {
            yield* [1, 2, 3, 4, 5];
        }
        const stream = new FluentAsyncIterator(source());
        assert.deepEqual(await stream.collect(), [1, 2, 3, 4, 5])
    });

    it('batches', async () => {
        async function* source() {
            yield* [1, 2, 3, 4, 5];
        }
        const stream = new FluentAsyncIterator(source());
        assert.deepEqual(await stream.batch(2).collect(), [
            [1, 2],
            [3, 4],
            [5]
        ]);
    });

    it('groups sorted objects by given key', async () => {
        async function* objectsSortedByKey() {
            yield* [
                { foo: '1', bar: 'a' },
                { foo: '1', bar: 'b' },
                { foo: '2', bar: 'c' },
                { foo: '3', bar: 'd' },
            ];
        }
        const stream = new FluentAsyncIterator(objectsSortedByKey());
        assert.deepEqual(await stream.group('foo').collect(), [
            { key: '1', group: [{ foo: '1', bar: 'a' }, { foo: '1', bar: 'b' }] },
            { key: '2', group: [{ foo: '2', bar: 'c' }] },
            { key: '3', group: [{ foo: '3', bar: 'd' }] }
        ]);
    });

    it('maps', async () => {
        async function* source() {
            yield* [1, 2, 3];
        }
        const stream = new FluentAsyncIterator(source());
        const results = await stream.map(x => x * 2).collect();
        assert.deepEqual(results, [2, 4, 6]);
    })

    it('returns underlying iterable', async () => {
        async function* source() {
            yield* [1, 2, 3];
        }
        const stream = new FluentAsyncIterator(source());
        const iterable = stream.map(x => x * 2).iterable();
        const results: any[] = [];
        for await (const item of iterable) {
            results.push(item);
        }
        assert.deepEqual(results, [2, 4, 6]);
    });

    it('filters', async () => {
        async function* source() {
            yield* [1, 2, 3];
        }
        const stream = new FluentAsyncIterator(source());
        const results = await stream.filter(x => x !== 2).collect();
        assert.deepEqual(results, [1, 3]);
    })

    it('filters nothing', async () => {
        async function* source() {
            yield* [];
        }
        const stream = new FluentAsyncIterator(source());
        const results = await stream.filter(x => x !== 2).collect();
        assert.deepEqual(results, []);
    })

    it('is lazy', async () => {
        let generatorInvoked = false
        async function* source() {
            generatorInvoked = true
            yield 2
        }
        let stream = iterator(source())
        assert(!generatorInvoked)
        stream = stream.map(x => x + x)
        assert(!generatorInvoked)
        const result = await stream.collect()
        assert(generatorInvoked)
        assert.deepEqual(result, [4])
    })

});
