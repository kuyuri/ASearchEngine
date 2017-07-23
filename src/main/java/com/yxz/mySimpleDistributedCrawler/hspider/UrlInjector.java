package com.yxz.mySimpleDistributedCrawler.hspider;

import java.util.List;

import com.yxz.mySimpleDistributedCrawler.utils.HBaseUtil;

/**
 *  @author Yu   
 */
public class UrlInjector {
	
	
	public void injectUrls(List<String> urls) {
		try {
			for(String url : urls) {
				HBaseUtil.addRecord("url", url, "info", "status", "0");
			}
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public void injectUrl(String url) {
		try {
			HBaseUtil.addRecord("url", url, "info", "status", "0");
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public static void main(String[] args) {
		new UrlInjector().injectUrl("www.zhihu.com");
	}

}
