package org.utils.cedis

import com.google.common.collect.Lists
import org.utils.cedis.commands.ExecutionStrategy
import org.utils.cedis.commands.IndividualExecution
import org.utils.cedis.commands.PipelinedExecution
import org.slf4j.LoggerFactory
import redis.clients.jedis.Jedis
import redis.clients.jedis.JedisPool
import redis.clients.jedis.Pipeline
import redis.clients.jedis.exceptions.JedisException
import java.util.Queue
import java.util.function.Function

class RedisCommandBuilder<T> private constructor(private val jedisPool: JedisPool, isPipelined: Boolean) {
    private val executionStrategy: ExecutionStrategy<T>
    private var errorMessage: String? = null
    private val operations: Queue<Function<T, Any>>

    init {
        this.errorMessage = "Error while executing redis command."
        this.operations = Lists.newLinkedList()
        this.executionStrategy = if (isPipelined) PipelinedExecution() else IndividualExecution()
    }

    fun fetch(): List<Any>? {
        return fetch(Any::class.java)
    }

    fun <R> fetch(responseClass: Class<R>): List<R>? {
        return safelyReturn({ jedis -> executionStrategy.fetch(jedis, responseClass, operations) } as Function<Jedis, List<R>>)
    }

    fun fetchOne(): Any? {
        return fetchOne(Any::class.java)
    }

    fun <R> fetchOne(responseClass: Class<R>): R? {
        val responses = fetch()
        return if (responses != null && !responses.isEmpty()) {
            responseClass.cast(responses[0])
        } else null
    }

    fun execute() {
        fetch()
    }

    private fun <R> safelyReturn(f: Function<Jedis, R>): R? {
        try {
            jedisPool.resource.use { jedis -> return f.apply(jedis) }
        } catch (e: JedisException) {
            LOGGER.error(errorMessage, e)
            return null
        }

    }

    fun setErrorMessage(message: String): RedisCommandBuilder<*> {
        this.errorMessage = message
        return this
    }

    fun expire(key: String, expiration: Int): RedisCommandBuilder<*> {
        return doOperation(executionStrategy.expire(key, expiration))
    }

    fun expireAt(key: String, unixTime: Long): RedisCommandBuilder<*> {
        return doOperation(executionStrategy.expireAt(key, unixTime))
    }

    fun sadd(key: String, elements: Array<String>): RedisCommandBuilder<*> {
        return doOperation(executionStrategy.sadd(key, elements))
    }

    fun del(key: String): RedisCommandBuilder<*> {
        return doOperation(executionStrategy.del(key))
    }

    fun sadd(key: String, elements: Array<String>, partitionSize: Int): RedisCommandBuilder<*> {
        operations.addAll(executionStrategy.sadd(key, elements, partitionSize))
        return this
    }

    fun srem(key: String, elements: Array<String>): RedisCommandBuilder<*> {
        return doOperation(executionStrategy.srem(key, elements))
    }

    fun srem(key: String, elements: Array<String>, partitionSize: Int): RedisCommandBuilder<*> {
        operations.addAll(executionStrategy.srem(key, elements, partitionSize))
        return this
    }

    operator fun get(key: String): RedisCommandBuilder<*> {
        return doOperation(executionStrategy[key])
    }

    fun hget(key: String, field: String): RedisCommandBuilder<*> {
        return doOperation(executionStrategy.hget(key, field))
    }

    fun hgetAll(key: String): RedisCommandBuilder<*> {
        return doOperation(executionStrategy.hgetAll(key))
    }

    fun getSet(key: String, value: String): RedisCommandBuilder<*> {
        return doOperation(executionStrategy.getSet(key, value))
    }

    operator fun set(key: String, value: String): RedisCommandBuilder<*> {
        return doOperation(executionStrategy.set(key, value))
    }

    fun hmset(key: String, hash: Map<String, String>): RedisCommandBuilder<*> {
        return doOperation(executionStrategy.hmset(key, hash))
    }

    fun smembers(key: String): RedisCommandBuilder<*> {
        return doOperation(executionStrategy.smembers(key))
    }

    fun incrBy(key: String, amount: Long): RedisCommandBuilder<*> {
        return doOperation(executionStrategy.incrBy(key, amount))
    }

    fun incr(key: String): RedisCommandBuilder<*> {
        return doOperation(executionStrategy.incr(key))
    }

    fun hincrBy(key: String, field: String, value: Long): RedisCommandBuilder<*> {
        return doOperation(executionStrategy.hincrBy(key, field, value))
    }

    fun eval(script: String, numKeys: Int, vararg args: String): RedisCommandBuilder<*> {
        return doOperation(executionStrategy.eval(script, numKeys, *args))
    }

    fun sismember(key: String, member: String): RedisCommandBuilder<*> {
        return doOperation(executionStrategy.sismember(key, member))
    }

    fun zadd(key: String, score: Double?, member: String): RedisCommandBuilder<*> {
        return doOperation(executionStrategy.zadd(key, score, member))
    }

    private fun doOperation(operation: Function<T, Any>): RedisCommandBuilder<*> {
        operations.add(operation)
        return this
    }

    fun flushAll() {
        safelyReturn(Function<Jedis, String> { it.flushAll() })
    }

    companion object {
        private val LOGGER = LoggerFactory.getLogger(RedisCommandBuilder<*>::class.java)

        fun from(jedisPool: JedisPool, isPipelined: Boolean): RedisCommandBuilder<*> {
            return if (isPipelined) RedisCommandBuilder<Pipeline>(jedisPool, true) else RedisCommandBuilder<Jedis>(jedisPool, false)
        }
    }
}