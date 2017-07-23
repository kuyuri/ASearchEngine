package com.yxz.mySimpleDistributedCrawler.utils;

import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.AbstractSet;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.yxz.mySimpleDistributedCrawler.hspider.RedisNode;

import redis.clients.jedis.JedisCluster;

/**
* @author Yu 
*/
public class BloomFilterRedisUtil {
	
	private static final Logger logger = LoggerFactory.getLogger(BloomFilterRedisUtil.class);
	
	private String key = "urls";
	
	private RedisUtil redisUtil = RedisUtil.getInstance(); 
	
	private int hashes = 5;                  // number of hash
	
	private int setSize = 1024 * 1024; 			// total size
	
	private MessageDigest md5;		// md5
	
	public BloomFilterRedisUtil(int setSize, int hashes, String key) {
		this.setSize = setSize;
		this.key = key;
		this.hashes = hashes;
		try {
			md5 = MessageDigest.getInstance("MD5");
		} catch (NoSuchAlgorithmException e) {
			throw new IllegalArgumentException("MD5 Hash not found");
		}
	}
	
	public BloomFilterRedisUtil() {
		try {
			md5 = MessageDigest.getInstance("MD5");
		} catch (NoSuchAlgorithmException e) {
			throw new IllegalArgumentException("MD5 Hash not found");
		}
	}
	
	private int getHash(int i) {
		md5.reset();
		byte[] bytes = ByteBuffer.allocate(4).putInt(i).array();
		md5.update(bytes, 0, bytes.length);
		return Math.abs(new BigInteger(1, md5.digest()).intValue()) % (setSize - 1);
	}
	
	private int[] getSetArray(String url) {
		int[] toSet = new int[hashes];
		toSet[0] = getHash(url.hashCode());
		for (int i = 1; i < hashes; i++)
			toSet[i] = (getHash(toSet[i - 1])); // hash function
		return toSet;
	}
	
	public boolean add(String url) {
		int[] toSet = getSetArray(url);
		for (int x : toSet)
			redisUtil.setBit(key, x, url);
		return true;
	}	
	
	public boolean contains(String url) {
		int[] toSet = getSetArray(url);
		for (int x : toSet) {
			if(!redisUtil.getBit(key, x, url))
				return false;
		}
		return true;
	}
	
	public void clear() {
		redisUtil.clearBitMap(key);
	}

	public static void main(String[] args) {
		RedisUtil redisUtil = RedisUtil.getInstance();
		List<RedisNode> redisNodeList = new ArrayList<>();
		redisNodeList.add(new RedisNode("127.0.0.1", 6379));
		redisNodeList.add(new RedisNode("127.0.0.1", 6380));
		redisUtil.setRedisNodeList(redisNodeList);
		BloomFilterRedisUtil urlFilter = new BloomFilterRedisUtil();
		logger.info("" + urlFilter.contains("www.zhihu.com"));
		urlFilter.add("www.zhihu.com");
		logger.info("" + urlFilter.contains("www.zhihu.com"));
	}
	
}

