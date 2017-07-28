package com.jzsec.rtc.util;

import com.jzsec.rtc.config.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

/**
 * Created by caodaoxi on 16-7-11.
 */
public class RedisLookUp {
//    private static Jedis jedis = new Jedis(Configuration.getConfig().getString("redis.ip"), Configuration.getConfig().getInt("redis.port"));
    private static JedisPool pool = null;
    private static final Logger LOG = LoggerFactory.getLogger(RedisLookUp.class);
//    private static Jedis jedis = null;
    static {
        JedisPoolConfig config = new JedisPoolConfig();
        config.setMaxIdle(20);
        config.setMaxTotal(50);
        config.setTestOnBorrow(true);
        config.setTestOnReturn(true);
        config.setTestWhileIdle(true);

        String host = Configuration.getConfig().getString("redis.ip");
        int port = Configuration.getConfig().getInt("redis.port");
        pool = new JedisPool(config, host, port, 100);
    }
    public static Object get(String key) {
        Object reval = null;
        Jedis jedis = null;
        try {
            jedis = pool.getResource();
            reval = jedis.get(key);
        } catch (Exception e) {
            LOG.info("Error, idle : " + pool.getNumIdle() + ", active : " + pool.getNumActive() + ", waiter : " + pool.getNumWaiters());
        } finally {
            if(jedis != null) jedis.close();
        }
        return reval;
    }



    public static void main(String[] args) throws InterruptedException {
        for(int i = 0; i < 20; i++) {
            new Thread(new Runnable() {
                @Override
                public void run() {
                    while (true) {
                        System.out.println(RedisLookUp.get("kg_asset:180010552046"));
//                        System.out.println("kg_asset:180010552046");
                    }
                }
            }).start();
        }
    }

}
