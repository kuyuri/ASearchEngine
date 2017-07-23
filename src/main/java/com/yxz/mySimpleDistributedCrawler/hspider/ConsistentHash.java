package com.yxz.mySimpleDistributedCrawler.hspider;

import java.security.MessageDigest;
import java.util.List;
import java.util.SortedMap;
import java.util.TreeMap;

import com.yxz.mySimpleDistributedCrawler.utils.MD5HashUtil;

/**
 * consistent hash  
 *  @author Yu   
 */
public class ConsistentHash {
	
	// number of virture node's complication
	private static final int COMPLICATION_NUM = 64;
	
	// the node circle implemented by rb-tree
	private static SortedMap<Integer, RedisNode> circle = new TreeMap<>();
	
	public ConsistentHash(List<RedisNode> redisNodes) {
		for(RedisNode node : redisNodes) {
			addNode(node);
		}
	}

	public void addNode(RedisNode node) {
		for(int i = 0; i < COMPLICATION_NUM; i++) {
			circle.put((MD5HashUtil.hash(node.getIp()) + i).hashCode(), node);
		}
	}
	
	public void removeNode(RedisNode node) {
		for(int i = 0; i < COMPLICATION_NUM; i++) {
			circle.remove((MD5HashUtil.hash(node.getIp()) + i).hashCode());
		}
	}
	
	public RedisNode getNode(String key) {
		if(circle.isEmpty()) {
			return null;
		}
		if(!circle.containsKey(MD5HashUtil.hash(key).hashCode())) {
			SortedMap<Integer, RedisNode> tailMap = circle.tailMap(MD5HashUtil.hash(key).hashCode());
			if(tailMap.isEmpty()) {
				return circle.get(circle.firstKey());
			}
			else {
				return circle.get(tailMap.firstKey());
			}
		}
		return circle.get(key);
	}
	
}
