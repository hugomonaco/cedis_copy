package com.grindr.storage.redis

import com.grindr.storage.redis.commands.ExecutionStrategy
import com.grindr.storage.redis.commands.Executors
import com.grindr.storage.redis.commands.IndividualExecution
import com.grindr.storage.redis.commands.OperationQueue
import com.grindr.storage.redis.commands.PipelinedExecution
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import redis.clients.jedis.BinaryJedis
import redis.clients.jedis.Jedis
import redis.clients.jedis.JedisPool
import redis.clients.jedis.Pipeline
import redis.clients.jedis.exceptions.JedisException
import java.util.function.Function

class RedisCommandBuilder private constructor(private val jedisPool: JedisPool, isPipelined: Boolean) {
    private val executionStrategy: ExecutionStrategy
    private var errorMessage: String? = null
    private val operations: OperationQueue

    init {
        this.errorMessage = "Error while executing redis command."
        this.operations = if (isPipelined) OperationQueue<Pipeline>() else OperationQueue<Jedis>()
        this.executionStrategy = if (isPipelined) PipelinedExecution() else IndividualExecution()
    }

    fun fetch(): List<Any>? {
        return fetch(Any::class.java)
    }

    fun <R> fetch(responseClass: Class<R>): List<R>? {
        return safelyReturn<List<R>>({ jedis -> executionStrategy.fetch(jedis, responseClass, operations) })
    }

    fun fetchOne(): Any? {
        return fetchOne<Any>(Any::class.java)
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
            jedisPool.getResource().use({ jedis -> return f.apply(jedis) })
        } catch (e: JedisException) {
            LOGGER.error(errorMessage, e)
            return null
        }

    }

    fun setErrorMessage(message: String): RedisCommandBuilder {
        this.errorMessage = message
        return this
    }

    fun expire(key: String, expiration: Int): RedisCommandBuilder {
        return doOperation(executionStrategy.expire(key, expiration))
    }

    fun expireAt(key: String, unixTime: Long): RedisCommandBuilder {
        return doOperation(executionStrategy.expireAt(key, unixTime))
    }

    fun sadd(key: String, elements: Array<String>): RedisCommandBuilder {
        return doOperation(executionStrategy.sadd(key, elements))
    }

    fun del(key: String): RedisCommandBuilder {
        return doOperation(executionStrategy.del(key))
    }

    fun sadd(key: String, elements: Array<String>, partitionSize: Int): RedisCommandBuilder {
        operations.addAll(executionStrategy.sadd(key, elements, partitionSize))
        return this
    }

    fun srem(key: String, elements: Array<String>): RedisCommandBuilder {
        return doOperation(executionStrategy.srem(key, elements))
    }

    fun srem(key: String, elements: Array<String>, partitionSize: Int): RedisCommandBuilder {
        operations.addAll(executionStrategy.srem(key, elements, partitionSize))
        return this
    }

    operator fun get(key: String): RedisCommandBuilder {
        return doOperation(executionStrategy.get(key))
    }

    fun hget(key: String, field: String): RedisCommandBuilder {
        return doOperation(executionStrategy.hget(key, field))
    }

    fun hgetAll(key: String): RedisCommandBuilder {
        return doOperation(executionStrategy.hgetAll(key))
    }

    fun getSet(key: String, value: String): RedisCommandBuilder {
        return doOperation(executionStrategy.getSet(key, value))
    }

    operator fun set(key: String, value: String): RedisCommandBuilder {
        return doOperation(executionStrategy.set(key, value))
    }

    fun hmset(key: String, hash: Map<String, String>): RedisCommandBuilder {
        return doOperation(executionStrategy.hmset(key, hash))
    }

    fun smembers(key: String): RedisCommandBuilder {
        return doOperation(executionStrategy.smembers(key))
    }

    fun incrBy(key: String, amount: Long): RedisCommandBuilder {
        return doOperation(executionStrategy.incrBy(key, amount))
    }

    fun incr(key: String): RedisCommandBuilder {
        return doOperation(executionStrategy.incr(key))
    }

    fun hincrBy(key: String, field: String, value: Long): RedisCommandBuilder {
        return doOperation(executionStrategy.hincrBy(key, field, value))
    }

    fun eval(script: String, numKeys: Int, vararg args: String): RedisCommandBuilder {
        return doOperation(executionStrategy.eval(script, numKeys, args))
    }

    fun sismember(key: String, member: String): RedisCommandBuilder {
        return doOperation(executionStrategy.sismember(key, member))
    }

    fun zadd(key: String, score: Double?, member: String): RedisCommandBuilder {
        return doOperation(executionStrategy.zadd(key, score, member))
    }

    private fun doOperation(operation: Function<Executors, Any>): RedisCommandBuilder {
        operations.add(operation)
        return this
    }

    fun flushAll() {
        safelyReturn(Function<Jedis, Any> { BinaryJedis.flushAll() })
    }

    companion object {
        private val LOGGER = LoggerFactory.getLogger(RedisCommandBuilder::class.java)

        fun from(jedisPool: JedisPool, isPipelined: Boolean): RedisCommandBuilder {
            return if (isPipelined) RedisCommandBuilder(jedisPool, true) else RedisCommandBuilder(jedisPool, false)
        }
    }
}