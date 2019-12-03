package org.utils.cedis.commands

import com.google.common.collect.Iterables
import com.google.common.collect.Lists
import redis.clients.jedis.Jedis
import redis.clients.jedis.Pipeline
import java.util.*

class PipelinedExecution : ExecutionStrategy<Pipeline>() {

    override fun expire(key: String, expiration: Int): (Pipeline) -> Unit {
        return { pipeline ->
            if (expiration > 0) {
                pipeline.expire(key, expiration)
            }
        }
    }

    override fun expireAt(key: String, unixTime: Long): (Pipeline) -> Unit {
        return { pipeline -> pipeline.expireAt(key, unixTime) }
    }

    override fun sadd(key: String, elements: Array<String>): (Pipeline) -> Unit {
        return { pipeline -> pipeline.sadd(key, *elements) }
    }

    override fun del(key: String): (Pipeline) -> Unit {
        return { pipeline -> pipeline.del(key) }
    }

    override fun sadd(key: String, elements: Array<String>, partitionSize: Int): Queue<(Pipeline) -> Unit> {
        val queue = Lists.newLinkedList<(Pipeline) -> Unit>()
        Iterables.partition(Arrays.asList(*elements), partitionSize).forEach { partition ->
            val elementsToAdd = Iterables.toArray(partition, String::class.java)
            queue.add(sadd(key, elementsToAdd))
        }
        return queue
    }

    override fun srem(key: String, elements: Array<String>): (Pipeline) -> Unit {
        return { pipeline -> pipeline.srem(key, *elements) }
    }

    override fun srem(key: String, elements: Array<String>, partitionSize: Int): Queue<(Pipeline) -> Unit> {
        val queue = Lists.newLinkedList<(Pipeline) -> Unit>()
        Iterables.partition(Arrays.asList(*elements), partitionSize).forEach { partition ->
            val elementsToAdd = Iterables.toArray(partition, String::class.java)
            queue.add(srem(key, elementsToAdd))
        }
        return queue
    }

    override operator fun get(key: String): (Pipeline) -> Unit {
        return { pipeline -> pipeline.get(key) }
    }

    override fun hget(key: String, field: String): (Pipeline) -> Unit {
        return { pipeline -> pipeline.hget(key, field) }
    }

    override fun hgetAll(key: String): (Pipeline) -> Unit {
        return { pipeline -> pipeline.hgetAll(key) }
    }

    override fun getSet(key: String, value: String): (Pipeline) -> Unit {
        return { pipeline -> pipeline.getSet(key, value) }
    }

    override operator fun set(key: String, value: String): (Pipeline) -> Unit {
        return { pipeline -> pipeline.set(key, value) }
    }

    override fun hmset(key: String, hash: Map<String, String>): (Pipeline) -> Unit {
        return { pipeline -> pipeline.hmset(key, hash) }
    }

    override fun smembers(key: String): (Pipeline) -> Unit {
        return { pipeline -> pipeline.smembers(key) }
    }

    override fun incrBy(key: String, amount: Long): (Pipeline) -> Unit {
        return { pipeline -> pipeline.incrBy(key, amount) }
    }

    override fun incr(key: String): (Pipeline) -> Unit {
        return { pipeline -> pipeline.incr(key) }
    }

    override fun hincrBy(key: String, field: String, value: Long): (Pipeline) -> Unit {
        return { pipeline -> pipeline.hincrBy(key, field, value) }
    }

    override fun eval(script: String, numKeys: Int, vararg args: String): (Pipeline) -> Unit {
        return { pipeline -> pipeline.eval(script, numKeys, *args) }
    }

    override fun sismember(key: String, member: String): (Pipeline) -> Unit {
        return { pipeline -> pipeline.sismember(key, member) }
    }

    override fun zadd(key: String, score: Double?, member: String): (Pipeline) -> Unit {
        return { pipeline -> pipeline.zadd(key, score!!, member) }
    }


    override fun fetch(jedis: Jedis, operations: Queue<(Pipeline) -> Unit>): List<Unit> {
        val results = Lists.newLinkedList<Unit>()
        val pipeline = jedis.pipelined()

        while (!operations.isEmpty()) {
            results.add(operations.poll().invoke(pipeline))
        }

        return results
    }
}
