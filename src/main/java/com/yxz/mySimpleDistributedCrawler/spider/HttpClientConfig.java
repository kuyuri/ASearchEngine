package com.yxz.mySimpleDistributedCrawler.spider;

import org.apache.http.client.HttpClient;
import org.apache.http.impl.client.DefaultHttpClient;

/**
* @author Yu 
* 产生HttpClient客户端和上下文
*/
public class HttpClientConfig {

	private static HttpClient client = new DefaultHttpClient();
//	private static HttpClientContext context = HttpClientContext.create();

    public static HttpClient getHttpClient() {
    	return client;
    }
    
//    public static HttpClientContext getHttpContext() {
//    	return context;
//    } 
    
}
