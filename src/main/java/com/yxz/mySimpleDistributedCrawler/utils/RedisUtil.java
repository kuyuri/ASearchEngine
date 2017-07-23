package com.yxz.mySimpleDistributedCrawler.utils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.yxz.mySimpleDistributedCrawler.hspider.ConsistentHash;
import com.yxz.mySimpleDistributedCrawler.hspider.RedisNode;

import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

/**
 *  @author Yu   
 */
public class RedisUtil {
	
	private static final Logger logger = LoggerFactory.getLogger(RedisUtil.class);
	
	//lazy mode singleton
	private static class InstanceHolder {
		private static final RedisUtil redisutil = new RedisUtil();
	}
	
	private RedisUtil() {
	
	}
	
	public static RedisUtil getInstance() {
		return InstanceHolder.redisutil;
	}
	
	private List<RedisNode> redisNodeList = new ArrayList<>();
	
	private Map<RedisNode, JedisPool> jedisPools = new HashMap<>();
	
	private ConsistentHash consistentHash;
	
	public List<RedisNode> getRedisNodeList() {
		return redisNodeList;
	}
	
	public void setRedisNodeList(List<RedisNode> redisNodeList) {
		this.redisNodeList = redisNodeList;
		this.consistentHash = new ConsistentHash(redisNodeList);
		JedisPoolConfig config = new JedisPoolConfig();
        config.setMaxIdle(RedisConfig.MAX_IDLE);
        config.setTestOnBorrow(true);
        config.setTestOnReturn(true);
        for(RedisNode node : redisNodeList) {
	        JedisPool jedisPool = new JedisPool(config, node.getIp(), node.getPort(), RedisConfig.TIMEOUT);
	        jedisPools.put(node, jedisPool);
        }
	}
	
	public boolean getBit(String key, int offset, String url) {
		Jedis jedis = null;
		try {
			RedisNode node = consistentHash.getNode(url);
			JedisPool jedisPool = jedisPools.get(node);
			jedis = jedisPool.getResource();
			return jedis.getbit(key, offset);
		} finally {
			if(jedis != null) {
				jedis.close();
			}
		}
	}
	
	public void setBit(String key, int offset, String url) {
		Jedis jedis = null;
		try {
			RedisNode node = consistentHash.getNode(url);
			JedisPool jedisPool = jedisPools.get(node);
			jedis = jedisPool.getResource();
			jedis.setbit(key, offset, true);
		} finally {
			if(jedis != null) {
				jedis.close();
			}
		}
	}
	
	public void deleteBit(String key, int offset, String url) {
		Jedis jedis = null;
		try {
			RedisNode node = consistentHash.getNode(url);
			JedisPool jedisPool = jedisPools.get(node);
			jedis = jedisPool.getResource();
			jedis.setbit(key, offset, false);
		} finally {
			if(jedis != null) {
				jedis.close();
			}
		}
	}
	
	public void clearBitMap(String key) {
		Jedis jedis = null;
		for(RedisNode node : redisNodeList) {
			try {
				JedisPool jedisPool = jedisPools.get(node);
				jedis = jedisPool.getResource();
				jedis.del(key);
			} finally {
				if(jedis != null) {
					jedis.close();
				}
			}
		}
	}
	
	public static void main(String[] args) {
		RedisUtil redisUtil = RedisUtil.getInstance();
		List<RedisNode> redisNodeList = new ArrayList<>();
		redisNodeList.add(new RedisNode("127.0.0.1", 6379));
		redisNodeList.add(new RedisNode("127.0.0.1", 6380));
		redisUtil.setRedisNodeList(redisNodeList);
		logger.info("" + redisUtil.getBit("a", 10, "www.qq.com"));
		redisUtil.setBit("a", 10, "www.qq.com");
		logger.info("" + redisUtil.getBit("a", 10, "www.qq.com"));
	}

}
