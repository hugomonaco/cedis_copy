package org.utils.cedis

import com.google.common.collect.Lists
import org.utils.cedis.commands.ExecutionStrategy
import org.slf4j.LoggerFactory
import redis.clients.jedis.Jedis
import redis.clients.jedis.JedisPool
import redis.clients.jedis.exceptions.JedisException
import java.util.Queue
import java.util.function.Function

class CedisBuilder<T> (private val jedisPool: JedisPool, private val  executionStrategy: ExecutionStrategy<T>) {
    private var errorMessage: String? = "Error while executing redis command."
    private val operations: Queue<(T) -> Unit> = Lists.newLinkedList();

    companion object Factory {
        private val LOGGER = LoggerFactory.getLogger("")
        fun <T> build(jedisPool: JedisPool, executionStrategy: ExecutionStrategy<T>): CedisBuilder<T> {
            var cedisBuilder : CedisBuilder<T> ? = null
            if(cedisBuilder == null){
                cedisBuilder = CedisBuilder(jedisPool, executionStrategy)
            }
            return cedisBuilder
        }
    }

//        fun from(): CedisBuilder<T> {
//            return if (isPipelined) CedisBuilder<T>(jedisPool, ExecutionStrategy<T>) else CedisBuilder<T>(jedisPool, ExecutionStrategy<T>)
//        }

        lateinit var instance: CedisBuilder<T>
            private set


        fun fetch(): List<Any>? {
            return fetch(Any::class.java)
        }

        fun <R> fetch(responseClass: Class<R>): List<R>? {
            return safelyReturn({ jedis:Jedis -> executionStrategy.fetch(jedis,  operations) } as Function<Jedis, List<R>>)
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

        fun setErrorMessage(message: String): CedisBuilder<T> {
            this.errorMessage = message
            return this
        }

        fun expire(key: String, expiration: Int): CedisBuilder<T> {
            return doOperation(executionStrategy.expire(key, expiration))
        }

        fun expireAt(key: String, unixTime: Long): CedisBuilder<T> {
            return doOperation(executionStrategy.expireAt(key, unixTime))
        }

        fun sadd(key: String, elements: Array<String>): CedisBuilder<T> {
            return doOperation(executionStrategy.sadd(key, elements))
        }

        fun del(key: String): CedisBuilder<T> {
            return doOperation(executionStrategy.del(key))
        }

        fun sadd(key: String, elements: Array<String>, partitionSize: Int): CedisBuilder<T> {
            operations.addAll(executionStrategy.sadd(key, elements, partitionSize))
            return this
        }

        fun srem(key: String, elements: Array<String>): CedisBuilder<T> {
            return doOperation(executionStrategy.srem(key, elements))
        }

        fun srem(key: String, elements: Array<String>, partitionSize: Int): CedisBuilder<T> {
            operations.addAll(executionStrategy.srem(key, elements, partitionSize))
            return this
        }

        operator fun get(key: String): CedisBuilder<T> {
            return doOperation(executionStrategy[key])
        }

        fun hget(key: String, field: String): CedisBuilder<T> {
            return doOperation(executionStrategy.hget(key, field))
        }

        fun hgetAll(key: String): CedisBuilder<T> {
            return doOperation(executionStrategy.hgetAll(key))
        }

        fun getSet(key: String, value: String): CedisBuilder<T> {
            return doOperation(executionStrategy.getSet(key, value))
        }

        operator fun set(key: String, value: String): CedisBuilder<T> {
            return doOperation(executionStrategy.set(key, value))
        }

        fun hmset(key: String, hash: Map<String, String>): CedisBuilder<T> {
            return doOperation(executionStrategy.hmset(key, hash))
        }

        fun smembers(key: String): CedisBuilder<T> {
            return doOperation(executionStrategy.smembers(key))
        }

        fun incrBy(key: String, amount: Long): CedisBuilder<T> {
            return doOperation(executionStrategy.incrBy(key, amount))
        }

        fun incr(key: String): CedisBuilder<T> {
            return doOperation(executionStrategy.incr(key))
        }

        fun hincrBy(key: String, field: String, value: Long): CedisBuilder<T> {
            return doOperation(executionStrategy.hincrBy(key, field, value))
        }

        fun eval(script: String, numKeys: Int, vararg args: String): CedisBuilder<T> {
            return doOperation(executionStrategy.eval(script, numKeys, *args))
        }

        fun sismember(key: String, member: String): CedisBuilder<T> {
            return doOperation(executionStrategy.sismember(key, member))
        }

        fun zadd(key: String, score: Double?, member: String): CedisBuilder<T> {
            return doOperation(executionStrategy.zadd(key, score, member))
        }

        private fun doOperation(operation: (T) -> Unit): CedisBuilder<T> {
            operations.add(operation)
            return this
        }

        fun flushAll() {
            safelyReturn(Function<Jedis, String> { it.flushAll() })
        }

    }
