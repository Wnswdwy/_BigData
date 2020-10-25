package com.nswdwy.redis;


import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Protocol;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * @author yycstart
 * @create 2020-10-16 14:44
 */
public class RedisClientTest {

    Jedis jedis = null;
    @Before
    public void init(){
        String host = "hadoop102";
        int port = Protocol.DEFAULT_PORT;
        jedis = new Jedis(host,port);
    }
    /**
     *Redis关闭
     */
    @After
    public void close(){
        jedis.close();
    }

    /**
     * 测试Redis连接
     */
    @Test
    public void connectionTest(){
        String ping = jedis.ping();
        System.out.println(ping);
    }


    /**
     * 测试对key的操作
     */
    @Test
    public void testKey() {
        //获取所有的key
        Set<String> keys = jedis.keys("*");
        //遍历
        System.out.println("所有的key为：");
        for (String key : keys) {
            System.out.println(key);
        }
        //判断某个key是否存在
        Boolean exists = jedis.exists("k7");
        System.out.println("k7是否存在：" + exists);
        //查看key对应的值的类型
        String k6 = jedis.type("k6");
        System.out.println("k6对应的值的类型是：" + k6);
        //设置key的过期时间
        jedis.expire("k2", 10);
        //判断key的过期时间
        Long k2 = jedis.ttl("k2");
        System.out.println("key还有 " + k2 + " 秒过期");

    }

    /**
     * 测试string类型的数据
     */
    @Test
    public void testString() {
        //删库
        jedis.flushDB();
        //设置k1
        jedis.set("k1", "v1");
        //获取k1的值
        String k1 = jedis.get("k1");
        System.out.println("k1的值是：" + k1);
        //向k1的值后面追加内容
        jedis.append("k1", "123");
        //获取k1的值
        String appendK1 = jedis.get("k1");
        System.out.println("k1追加之后的值是：" + appendK1);
        //获取k1的值从0到2的值
        String k1range = jedis.getrange("k1", 0, 2);
        System.out.println(k1range);
        //批量设置值
        jedis.mset("k2", "v2", "k3", "v3");
        //批量获取
        List<String> mget = jedis.mget("k1", "k2", "k3");
        for (String s : mget) {
            System.out.println(s);
        }
        //设置key的同时设置过期时间
        jedis.setex("k4", 30, "v4");
        //睡3秒
        try {
            Thread.sleep(3000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        //获取k4的有效时间
        Long k4Expire = jedis.ttl("k4");
        System.out.println("k4的剩余时间：" + k4Expire);
    }

    /**
     * 测试list类型的数据
     */
    @Test
    public void testList() {
        //从左边添加数据
        jedis.lpush("k5", "l1", "l2", "l3");
        //从右边边添加数据
        jedis.rpush("k6", "l11", "l22", "l33");
        //从k5的左边吐出一个值
        String k5 = jedis.lpop("k5");
        System.out.println("从k5的左边吐出的一个值是："+k5);
        //从k6的右边吐出一个值
        String k6 = jedis.rpop("k6");
        System.out.println("从k6的右边吐出的一个值是："+k6);
        //获取k5种的所有的值
        List<String> k51 = jedis.lrange("k5", 0, -1);
        System.out.println("k5中所有的值是："+k51);
        //从k5的右边吐出一个，然后从左边插入到k6种
        jedis.rpoplpush("k5","k6");
        //获取k5种的所有的值
        List<String> k52 = jedis.lrange("k5", 0, -1);
        System.out.println("k5中所有的值是："+k52);
        //获取k5种的所有的值
        List<String> k61 = jedis.lrange("k6", 0, -1);
        System.out.println("k6中所有的值是："+k61);
    }

    /**
     * 测试set类型的数据
     */
    @Test
    public void testSet(){
        //设置
        jedis.sadd("s1","v1","v2","v2","v3","v3","v5","v5");
        //获取s1中所有的值
        Set<String> smembers = jedis.smembers("s1");
        System.out.println("s1中所有的值为：");
        for (String smember : smembers) {
            System.out.println(smember);
        }
        //判断某个值是否存在
        Boolean sismember = jedis.sismember("s1", "k3");
        System.out.println("k3这个值是否存在：" + sismember);
        //获取值的数据
        Long s1 = jedis.scard("s1");
        System.out.println("s1中值的数量为："+s1);
        //从s1中随机返回一个数
        String s11 = jedis.srandmember("s1");
        System.out.println("从s1中随机返回的一个值是："+s11);
        //从s1中删除v5
        jedis.srem("s1","v5");
        //从s1中随机吐出2个值
        Set<String> s12 = jedis.spop("s1", 2);
        System.out.println("从s1中随机吐出的两个值是：");
        for (String s : s12) {
            System.out.println(s);
        }
    }
    /**
     * 测试hash类型的数据
     */
    @Test
    public void testHash(){
        //添加id属性
        jedis.hset("h1","id","1001");
        //获取id属性值
        String id = jedis.hget("h1", "id");
        System.out.println("id属性值是："+id);
        //设置一个map
        Map<String, String> map = new HashMap();
        map.put("name","zhangsan");
        map.put("age","23");
        map.put("email","zhangsan@lisi.com");
        //批量设置属性值
        jedis.hmset("h1",map);
        //批量获取h1中的属性值
        List<String> hmget = jedis.hmget("h1", "id", "name", "age", "email");
        System.out.println("h1中所有的属性值是："+hmget);
        //获取所有的属性
        Set<String> keys = jedis.hkeys("h1");
        System.out.println("所有的属性为：");
        for (String key : keys) {
            System.out.println(key);
        }
        //获取所有的值
        List<String> h1 = jedis.hvals("h1");
        System.out.println("所有的值是："+h1);
        //给zhangsan的age加3
        jedis.hincrBy("h1","age",3);
        //加3之后再次获取age的值
        String age = jedis.hget("h1", "age");
        System.out.println("加3之后的值是："+age);
    }

    /**
     * 测试zset类型的数据
     */
    @Test
    public void testZset(){
        //创建一个Map
        Map<String,Double> map = new HashMap<String, Double>();
        map.put("zs",59.00);
        map.put("ls",99.00);
        map.put("wmz",66.00);
        map.put("zl",79.00);
        map.put("tq",66.00);
        map.put("wb",88.00);
        //添加数据
        jedis.zadd("z1",map);
        //获取所有的值
        Set<String> z1 = jedis.zrange("z1", 0, -1);
        System.out.println("z1中所有的值是：");
        for (String s : z1) {
            System.out.println(s);
        }
        //获取分数从60到90的成员
        Set<String> z11 = jedis.zrangeByScore("z1", 60.00, 90.00);
        System.out.println("分数大于等于60，小于等于90的成员为：");
        for (String s : z11) {
            System.out.println(s);
        }
        //获取成员的数量
        Long z12 = jedis.zcard("z1");
        System.out.println("一共 "+z12+"个成员");
        //获取某一个成员的排名
        Long zrank = jedis.zrank("z1", "wmz");
        System.out.println("王麻子的排名是："+(zrank+1));
        //删除wb
        jedis.zrem("z1","wb");
    }
}
