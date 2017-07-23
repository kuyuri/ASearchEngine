package com.yxz.mySimpleDistributedCrawler.spider;

import java.util.HashMap;
import java.util.Map;

/**
* @author Yu 
* 网页实体类
*/
public class Page {

	private String result;
	private String url;
	private Map<String, Object> headerMap = new HashMap<>();
	
	public static final String CONTENT_LENGTH = "len";
	public static final String URL = "url";
	
	public Page(String result, String url) {
		this.result = result;
		this.url = url;
	}

	public String getResult() {
		return result;
	}
	public String getUrl() {
		return url;
	}

	public void init() {
		headerMap.put(CONTENT_LENGTH, result.length());
		headerMap.put(URL, url);
	}
	
}
