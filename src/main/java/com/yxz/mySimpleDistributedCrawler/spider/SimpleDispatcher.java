package com.yxz.mySimpleDistributedCrawler.spider;

import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
* @author Yu 
* url分发器
*/
public class SimpleDispatcher implements Dispatcher {

	private static final Logger logger = LoggerFactory.getLogger(Dispatcher.class);
	
	private BlockingQueue<String> urls = new ArrayBlockingQueue<>(100000);
	
	private SimpleDispatcher() {
	}
	
	private static final Dispatcher dispatcher = new SimpleDispatcher(); 
	
	public static Dispatcher newDispatcher() {
		return dispatcher;
	}

	public String getUrl() {
		try {
			return urls.take();
		} catch (InterruptedException e) {
			logger.error("failed to get url from dispatcher", e);
		}
		return null;
	}

	public void addUrls(List<String> urls) {
		this.urls.addAll(urls);
	}
	
	public void addUrl(String url) {
		this.urls.add(url);
	}

}
