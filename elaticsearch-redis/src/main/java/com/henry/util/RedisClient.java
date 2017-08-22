package com.henry.util;

/**
 * 用户：hadoop
 * 日期：8/8/2017
 * 时间：9:32 PM
 */

import lombok.Setter;
import org.slf4j.Logger;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

import java.nio.charset.Charset;
import java.util.List;
import java.util.Map;
import java.util.Set;
public class RedisClient {
    private static final Logger log = org.slf4j.LoggerFactory.getLogger(RedisClient.class);
    @Setter
    JedisPool jedisPool;
    private static final int EXPIRE_SECONDS = 3600;
    private static final Charset UTF_8 = Charset.forName("UTF-8");

    public Jedis getJedisResource() {
        try {
            return jedisPool.getResource();
        } catch (Exception ex) {
            log.info("Cannot get Jedis connection", ex);
        }
        return null;
    }

    public void put(String key, String value) {
        Jedis jedis = getJedisResource();
        try {
            jedis.setex(key, EXPIRE_SECONDS, value);
        } finally {
            cleanJedisClient(jedis);
        }
    }

    public Set<String> keys(String pattern) {
        Jedis jedis = getJedisResource();
        try {
            return jedis.keys(pattern);
        } finally {
            cleanJedisClient(jedis);
        }
    }

    public void put(String key, String value, int expire) {
        Jedis jedis = getJedisResource();
        try {
            jedis.setex(key, expire, value);
        } finally {
            cleanJedisClient(jedis);
        }
    }

    public void putNotExpire(String key, String value) {
        Jedis jedis = getJedisResource();
        try {
            jedis.set(key, value);
        } finally {
            cleanJedisClient(jedis);
        }
    }

    public void expire(String key, int seconds) {
        Jedis jedis = getJedisResource();
        try {
            jedis.expire(key, seconds);
        } finally {
            cleanJedisClient(jedis);
        }
    }

    public Long incr(String key) {
        Jedis jedis = getJedisResource();
        try {
            return jedis.incr(key);
        } finally {
            cleanJedisClient(jedis);
        }
    }

    public Long decr(String key) {
        Jedis jedis = getJedisResource();
        try {
            return jedis.decr(key);
        } finally {
            cleanJedisClient(jedis);
        }
    }

    public void cleanJedisClient(Jedis jedis) {
        if (jedis != null) {
            jedis.close();
        }
    }

    public String get(String key) {
        Jedis jedis = getJedisResource();
        try {
            return jedis.get(key);
        } finally {
            cleanJedisClient(jedis);
        }
    }

    public byte[] get(byte[] key) {
        Jedis jedis = getJedisResource();
        try {
            long start = System.currentTimeMillis();
            byte[] result = jedis.get(key);
            long end = System.currentTimeMillis();
            return result;
        } finally {
            cleanJedisClient(jedis);
        }
    }

    public void remove(String key) {
        Jedis jedis = getJedisResource();
        try {
            jedis.del(key);
        } finally {
            cleanJedisClient(jedis);
        }
    }


    public void hset(String key, String field, String value) {
        Jedis jedis = getJedisResource();
        try {
            jedis.hset(key, field, value);
        } catch (Exception e) {
            log.error("jedis.hset exception", e);
        } finally {
            cleanJedisClient(jedis);
        }
    }

    public Long hsetnx(String key, String field, String value) {
        Jedis jedis = getJedisResource();
        try {
            return jedis.hsetnx(key, field, value);
        } catch (Exception e) {
            log.error("jedis.hsetnx exception", e);
        } finally {
            cleanJedisClient(jedis);
        }
        return null;
    }

    public Long setnx(String key, String value) {
        Jedis jedis = getJedisResource();
        try {
            return jedis.setnx(key, value);
        } catch (Exception e) {
            log.error("jedis.hsetnx exception", e);
        } finally {
            cleanJedisClient(jedis);
        }
        return null;
    }

    public void hmset(String key, Map<String, String> hash) {
        Jedis jedis = getJedisResource();
        try {
            jedis.hmset(key, hash);
        } catch (Exception e) {
            log.error("jedis.hmset exception", e);
        } finally {
            cleanJedisClient(jedis);
        }
    }

    public List<String> hmget(String key, List<String> fields) {
        Jedis jedis = getJedisResource();
        try {
            return jedis.hmget(key, fields.toArray(new String[fields.size()]));
        } catch (Exception e) {
            log.error("jedis.hmget exception", e);
        } finally {
            cleanJedisClient(jedis);
        }
        return null;
    }

    public List<String> mget(String... keys) {
        Jedis jedis = getJedisResource();
        try {
            return jedis.mget(keys);
        } catch (Exception e) {
            log.error("jedis.mget exception", e);
        } finally {
            cleanJedisClient(jedis);
        }
        return null;
    }

    public String mset(String... keysvalues) {
        Jedis jedis = getJedisResource();
        try {
            return jedis.mset(keysvalues);
        } catch (Exception e) {
            log.error("jedis.mset exception", e);
        } finally {
            cleanJedisClient(jedis);
        }
        return null;
    }

    public boolean exists(String key) {
        Jedis jedis = getJedisResource();
        try {
            return jedis.exists(key);
        } finally {
            cleanJedisClient(jedis);
        }
    }
}
