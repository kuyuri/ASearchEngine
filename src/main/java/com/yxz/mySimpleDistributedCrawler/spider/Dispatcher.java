package com.yxz.mySimpleDistributedCrawler.spider;

import java.util.List;

/**
* @author Yu 
* url分发统一接口
*/
public interface Dispatcher {
	
	public void addUrls(List<String> urls);
	public void addUrl(String url);
	public String getUrl();

}
