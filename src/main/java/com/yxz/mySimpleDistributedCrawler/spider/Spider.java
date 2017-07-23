package com.yxz.mySimpleDistributedCrawler.spider;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
* @author Yu 
* 爬虫启动器
*/
public class Spider {

	private int num = 4; //抓取网页的线程数
	private ExecutorService threadPool = Executors.newFixedThreadPool(num);
	
	private void start() {
		Dispatcher dispatcher = SimpleDispatcher.newDispatcher();
		dispatcher.addUrl("http://www.qq.com");
		for(int i = 0; i < num; i++) {
			threadPool.execute(new Fetcher(dispatcher));
		}
	}
	
	public static void main(String[] args) {
		new Spider().start();
	}
	
}
