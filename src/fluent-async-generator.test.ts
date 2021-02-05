import { strict as assert } from "assert";
import { toGroupedGenerator, batchGenerator, FluentAsyncGenerator } from "./fluent-async-generator";

describe('toGroupedGenerator', () => {
    it('groups sorted objects by given key and returns them as they are iterated', async () => {
        async function* objectsSortedByKey() {
            yield* [
                { foo: '1', bar: 'a' },
                { foo: '1', bar: 'b' },
                { foo: '2', bar: 'c' },
                { foo: '3', bar: 'd' },
            ];
        }
        const groupedGenerator = toGroupedGenerator(objectsSortedByKey, 'foo');
        const results: any[] = [];
        for await (const group of groupedGenerator()) {
            results.push(group);
        }
        assert.deepEqual(results, [
            { key: '1', group: [{ foo: '1', bar: 'a' }, { foo: '1', bar: 'b' }] },
            { key: '2', group: [{ foo: '2', bar: 'c' }] },
            { key: '3', group: [{ foo: '3', bar: 'd' }] }
        ]);
    });

    it('handles empty generator', async () => {
        async function* objectsSortedByKey() {
            yield* [];
        }
        const groupedGenerator = toGroupedGenerator(objectsSortedByKey, 'foo');
        const results: any[] = [];
        for await (const group of groupedGenerator()) {
            results.push(group);
        }
        assert.deepEqual(results, []);
    });
});

describe('batchGenerator', () => {
    it('creates a generator of batches', async () => {
        async function* source() {
            yield* [1, 2, 3, 4, 5];
        }
        const batched = batchGenerator(source, 2);
        const results: any[] = [];
        for await (const batch of batched()) {
            results.push(batch);
        }
        assert.deepEqual(results, [
            [1, 2],
            [3, 4],
            [5]
        ]);
    });

    it('handles empty generator', async () => {
        async function* source() {
            yield* [];
        }
        const batched = batchGenerator(source, 2);
        const results: any[] = [];
        for await (const batch of batched()) {
            results.push(batch);
        }
        assert.deepEqual(results, []);
    });
});

describe('FluentAsyncGenerator', () => {
    it('collects results', async () => {
        async function* source() {
            yield* [1, 2, 3, 4, 5];
        }
        const stream = new FluentAsyncGenerator(source);
        assert.deepEqual(await stream.collect(), [1, 2, 3, 4, 5])
    });

    it('batches', async () => {
        async function* source() {
            yield* [1, 2, 3, 4, 5];
        }
        const stream = new FluentAsyncGenerator(source);
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
        const stream = new FluentAsyncGenerator(objectsSortedByKey);
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
        const stream = new FluentAsyncGenerator(source);
        const results = await stream.map(x => x * 2).collect();
        assert.deepEqual(results, [2, 4, 6]);
    })

    it('returns underlying generator', async () => {
        async function* source() {
            yield* [1, 2, 3];
        }
        const stream = new FluentAsyncGenerator(source);
        const generator = stream.map(x => x * 2).generator();
        const results: any[] = [];
        for await (const item of generator()) {
            results.push(item);
        }
        assert.deepEqual(results, [2, 4, 6]);
    });

    it('filters', async () => {
        async function* source() {
            yield* [1, 2, 3];
        }
        const stream = new FluentAsyncGenerator(source);
        const results = await stream.filter(x => x !== 2).collect();
        assert.deepEqual(results, [1, 3]);
    })

    it('filters nothing', async () => {
        async function* source() {
            yield* [];
        }
        const stream = new FluentAsyncGenerator(source);
        const results = await stream.filter(x => x !== 2).collect();
        assert.deepEqual(results, []);
    })

});
