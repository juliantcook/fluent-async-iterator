import {promiseIterator} from "./fluent-async-iterator";
import {strict as assert} from "assert";

describe('FluentPromiseIterator', () => {
    it('can resolve promises concurrently', async () => {
        const delayTimes = [
            {id: 1, value: 1},
            {id: 2, value: 2},
            {id: 3, value: 3},
            {id: 4, value: 4},
            {id: 5, value: 1}
        ]
        const promises = delayTimes.map(async item => {
            await delay(item.value * 10)
            return item
        })
        const results = await promiseIterator(promises)
            .concurrentResolve(2)
            .limit(4)
            .collect()
        assert.deepEqual(results, [
            {id: 1, value: 1},
            {id: 2, value: 2},
            {id: 3, value: 3},
            {id: 5, value: 1},
        ])
    })

})

function delay(ms) {
    return new Promise(resolve => setTimeout(resolve, ms))
}
