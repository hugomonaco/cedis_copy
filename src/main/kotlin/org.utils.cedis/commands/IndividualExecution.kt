package org.utils.cedis.commands

import com.google.common.collect.Iterables
import com.google.common.collect.Lists
import redis.clients.jedis.Jedis

import java.util.Arrays
import java.util.Queue

class IndividualExecution : ExecutionStrategy<Jedis>() {

    override fun expire(key: String, expiration: Int): (Jedis) -> Unit {
        return { jedis ->
            if (expiration > 0) {
                jedis.expire(key, expiration)
            }
        }
    }

    override fun expireAt(key: String, unixTime: Long): (Jedis) -> Unit {
        return { jedis -> jedis.expireAt(key, unixTime) }
    }

    override fun sadd(key: String, elements: Array<String>): (Jedis) -> Unit {
        return { jedis -> jedis.sadd(key, *elements) }
    }

    override fun del(key: String): (Jedis) -> Unit {
        return { jedis -> jedis.del(key) }
    }

    override fun sadd(key: String, elements: Array<String>, partitionSize: Int): Queue<(Jedis) -> Unit> {
        val queue = Lists.newLinkedList<(Jedis) -> Unit>()
        Iterables.partition(Arrays.asList(*elements), partitionSize).forEach { partition ->
            val elementsToAdd = Iterables.toArray(partition, String::class.java)
            queue.add(sadd(key, elementsToAdd))
        }
        return queue
    }

    override fun srem(key: String, elements: Array<String>): (Jedis) -> Unit {
        return { jedis -> jedis.srem(key, *elements) }
    }

    override fun srem(key: String, elements: Array<String>, partitionSize: Int): Queue<(Jedis) -> Unit> {
        val queue = Lists.newLinkedList<(Jedis) -> Unit>()
        Iterables.partition(Arrays.asList(*elements), partitionSize).forEach { partition ->
            val elementsToAdd = Iterables.toArray(partition, String::class.java)
            queue.add(srem(key, elementsToAdd))
        }
        return queue
    }

    override operator fun get(key: String): (Jedis) -> Unit {
        return { jedis -> jedis.get(key) }
    }

    override fun hget(key: String, field: String): (Jedis) -> Unit {
        return { jedis -> jedis.hget(key, field) }
    }

    override fun hgetAll(key: String): (Jedis) -> Unit {
        return { jedis -> jedis.hgetAll(key) }
    }

    override fun getSet(key: String, value: String): (Jedis) -> Unit {
        return { jedis -> jedis.getSet(key, value) }
    }

    override operator fun set(key: String, value: String): (Jedis) -> Unit {
        return { jedis -> jedis.set(key, value) }
    }

    override fun hmset(key: String, hash: Map<String, String>): (Jedis) -> Unit {
        return { jedis -> jedis.hmset(key, hash) }
    }

    override fun smembers(key: String): (Jedis) -> Unit {
        return { jedis -> jedis.smembers(key) }
    }

    override fun incrBy(key: String, amount: Long): (Jedis) -> Unit {
        return { jedis -> jedis.incrBy(key, amount) }
    }

    override fun incr(key: String): (Jedis) -> Unit {
        return { jedis -> jedis.incr(key) }
    }

    override fun hincrBy(key: String, field: String, value: Long): (Jedis) -> Unit {
        return { jedis -> jedis.hincrBy(key, field, value) }
    }

    override fun eval(script: String, numKeys: Int, vararg args: String): (Jedis) -> Unit {
        return { jedis -> jedis.eval(script, numKeys, *args) }
    }

    override fun sismember(key: String, member: String): (Jedis) -> Unit {
        return { jedis -> jedis.sismember(key, member) }
    }

    override fun zadd(key: String, score: Double?, member: String): (Jedis) -> Unit {
        return { jedis -> jedis.zadd(key, score!!, member) }
    }

    override fun fetch(jedis: Jedis, operations: Queue<(Jedis) -> Unit>): List<Unit> {
        val results = Lists.newLinkedList<Unit>()
        while (!operations.isEmpty()) {
            results.add( operations.poll().invoke(jedis))
        }

        return results
    }
}
