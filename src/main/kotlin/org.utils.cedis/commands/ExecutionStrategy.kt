package org.utils.cedis.commands

import redis.clients.jedis.Jedis
import java.util.*

abstract class ExecutionStrategy<T> {
    //commands
    abstract fun expire(key: String, expiration: Int): (T) -> Unit

    abstract fun expireAt(key: String, unixTime: Long): (T) -> Unit
    abstract fun sadd(key: String, elements: Array<String>): (T) -> Unit
    abstract fun del(key: String): (T) -> Unit
    abstract fun sadd(key: String, elements: Array<String>, partitionSize: Int): Queue<(T) -> Unit>
    abstract fun srem(key: String, elements: Array<String>): (T) -> Unit
    abstract fun srem(key: String, elements: Array<String>, partitionSize: Int): Queue<(T) -> Unit>
    abstract operator fun get(key: String): (T) -> Unit
    abstract fun hget(key: String, field: String): (T) -> Unit
    abstract fun hgetAll(key: String): (T) -> Unit
    abstract fun getSet(key: String, value: String): (T) -> Unit
    abstract operator fun set(key: String, value: String): (T) -> Unit
    abstract fun hmset(key: String, hash: Map<String, String>): (T) -> Unit
    abstract fun smembers(key: String): (T) -> Unit
    abstract fun incrBy(key: String, amount: Long): (T) -> Unit
    abstract fun incr(key: String): (T) -> Unit
    abstract fun hincrBy(key: String, field: String, value: Long): (T) -> Unit
    abstract fun eval(script: String, numKeys: Int, vararg args: String): (T) -> Unit
    abstract fun sismember(key: String, member: String): (T) -> Unit
    abstract fun zadd(key: String, score: Double?, member: String): (T) -> Unit

    //fetch
    abstract fun <R> fetch(jedis: Jedis, responseClass: Class<R>, operations: Queue<(T) -> Unit>): List<R>
}
