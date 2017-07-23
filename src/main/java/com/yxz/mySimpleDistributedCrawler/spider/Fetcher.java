package com.yxz.mySimpleDistributedCrawler.spider;

import java.util.List;

import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.protocol.HttpClientContext;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
* @author Yu 
* 网页抓取执行器
*/
public class Fetcher implements Runnable {
	
	private static final Logger logger = LoggerFactory.getLogger(Fetcher.class);
		
	private Dispatcher dispatcher;
	private static CloseableHttpClient client = HttpClientConfig.getHttpClient(); 
	private static HttpClientContext context = HttpClientConfig.getHttpContext();
	
	public Fetcher(Dispatcher dispatcher) {
		this.dispatcher = dispatcher;
	}

	public Fetcher() {
		
	}
	
	public Page fetch(String url) {
		String result = null;
		HttpGet getMethod = null;
		Page page = null;
		try {
			getMethod = new HttpGet(url);
			getMethod.setHeader("User-Agent", "Mozilla/5.0 (Windows NT 6.3; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/44.0.2403.107 Safari/537.36");
			CloseableHttpResponse response = client.execute(getMethod, context); 
			result = EntityUtils.toString(response.getEntity());
			//statusCode = response.getStatusLine().getStatusCode();
			page = new Page(result, url);
		} catch (Exception e) {
			//logger.error("failed to fetch" + url, e);
		} finally {
			if(getMethod != null)
				getMethod.releaseConnection();
		}
		logger.info("successed to fetch" + url);
		return page;
	}
	
	public static String fetchHtml(String url) {
		String result = null;
		HttpGet getMethod = null;
		Page page = null;
		try {
			getMethod = new HttpGet(url);
			getMethod.setHeader("User-Agent", "Mozilla/5.0 (Windows NT 6.3; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/44.0.2403.107 Safari/537.36");
			CloseableHttpResponse response = client.execute(getMethod, context); 
			result = EntityUtils.toString(response.getEntity());
			//statusCode = response.getStatusLine().getStatusCode();
//			page = new Page(result, url);
		} catch (Exception e) {
			//logger.error("failed to fetch" + url, e);
		} finally {
			if(getMethod != null)
				getMethod.releaseConnection();
		}
		logger.info("successed to fetch" + url);
		return result;
	}

	private volatile boolean running = true;
	
	public void run() {
		while(running) {
			String url = dispatcher.getUrl();
//			logger.info("started to fetch " + url);
			Page page = this.fetch(url);      //抓取url对应的网页
			page.init();
			if(page != null) {
    			List<String> urls = Parser.parse(page);  //解析抓取到的网页
    			dispatcher.addUrls(urls);
    			DButils.storePageContent(page);
			}
//			Thread.sleep(100);  //避免抓取过于频繁被禁ip
		}
	}
	
}
