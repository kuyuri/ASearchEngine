package com.yxz.mySearchEngine.spider;

import org.apache.http.client.protocol.HttpClientContext;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;

/**
* @author Yu 
* 产生HttpClient客户端和上下文
*/
public class HttpClientConfig {

	private static CloseableHttpClient client = HttpClients.custom().build();
	private static HttpClientContext context = HttpClientContext.create();

    public static CloseableHttpClient getHttpClient() {
    	return client;
    }
    
    public static HttpClientContext getHttpContext() {
    	return context;
    } 
    
}
