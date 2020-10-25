package com.nswdwy.redis;


import org.junit.Test;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.Protocol;

/**
 * @author yycstart
 * @create 2020-10-16 15:06
 */
public class JedisPoolTest {
    /**
     * 测试使用连接池
     */
   @Test
    public void testJedisPool(){
       String host = "hadoop102";
       int port = Protocol.DEFAULT_PORT;
       JedisPool jedisPool = new JedisPool(host, port);
       Jedis resource = jedisPool.getResource();
       String ping = resource.ping();
       System.out.println(ping);

   }
}
