package com.yxz.mySimpleDistributedCrawler.utils;

import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.yxz.mySimpleDistributedCrawler.spider.Fetcher;
import com.yxz.mySimpleDistributedCrawler.spider.HttpClientConfig;
import com.yxz.mySimpleDistributedCrawler.spider.Page;

/**
 *  @author Yu   
 */
public class FetcherUtil {
	
	private static final Logger logger = LoggerFactory.getLogger(FetcherUtil.class);

	private static HttpClient client = HttpClientConfig.getHttpClient(); 
	
	public static String fetchHtml(String url) {
		String prefix = url.substring(0, 4);
		if(!"http".equalsIgnoreCase(prefix)) {
			url = "http://" + url;
		}
		String result = null;
		HttpGet getMethod = null;
		Page page = null;
		try {
			getMethod = new HttpGet(url);
			getMethod.setHeader("User-Agent", "Mozilla/5.0 (Windows NT 6.3; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/44.0.2403.107 Safari/537.36");
			HttpResponse response = client.execute(getMethod); 
			result = EntityUtils.toString(response.getEntity());
			//statusCode = response.getStatusLine().getStatusCode();
//			page = new Page(result, url);
		} catch (Exception e) {
			//logger.error("failed to fetch" + url, e);
		} finally {
//			client.getConnectionManager().shutdown();
		}
		logger.info("successed to fetch" + url);
		return result;
	}
	
	
	public static void main(String[] args) {
		System.out.println(fetchHtml("www.qq.com"));
	}

}
