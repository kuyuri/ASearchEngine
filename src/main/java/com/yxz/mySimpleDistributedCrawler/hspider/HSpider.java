package com.yxz.mySimpleDistributedCrawler.hspider;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.yxz.mySimpleDistributedCrawler.utils.BloomFilterRedisUtil;
import com.yxz.mySimpleDistributedCrawler.utils.Constants;
import com.yxz.mySimpleDistributedCrawler.utils.HBaseUtil;
import com.yxz.mySimpleDistributedCrawler.utils.RedisUtil;

/**
 *  @author Yu   
 */
public class HSpider {

	private static final Logger logger = LoggerFactory.getLogger(HSpider.class);
	
	public static final int CRAWL_DEPTH;
	public static final List<String> rootUrls;
	
	static {
		CRAWL_DEPTH = 2;
		rootUrls = Arrays.asList("www.qq.com");
	}
	
	private UrlInjector urlInjector = new UrlInjector();      //injecter
	
	private HFetcher hFetcher = new HFetcher();              //fetcher
	
	private HParser hParser = new HParser();                 //parser
	
	private static BloomFilterRedisUtil urlBloomFilter;
	
	static {
		RedisUtil redisUtil = RedisUtil.getInstance();
		List<RedisNode> redisNodeList = new ArrayList<>();
		redisNodeList.add(new RedisNode("127.0.0.1", 6379));
		redisNodeList.add(new RedisNode("127.0.0.1", 6380));
		redisNodeList.add(new RedisNode("127.0.0.1", 6381));
		redisUtil.setRedisNodeList(redisNodeList);
		urlBloomFilter = new BloomFilterRedisUtil();
	}
	
	public static BloomFilterRedisUtil getUrlBloomFilter() {
		return urlBloomFilter;
	}
	
	public static void main(String[] args) throws Exception {
		HBaseUtil.deleteTables();
		HBaseUtil.initTables();		
		HSpider hSpider = new HSpider();
		hSpider.urlInjector.injectUrls(rootUrls);
		for(int i = 0; i < CRAWL_DEPTH; i++) {
			hSpider.hFetcher.main(null); // start to fetch on hadoop
			hSpider.hParser.main(null); // start to parse html on hadoop
			HBaseUtil.deleteTable(Constants.cacheDocumentTableName); // clear document cache
		}
		logger.info("succeed to crawl : " + CRAWL_DEPTH);
	}

}
