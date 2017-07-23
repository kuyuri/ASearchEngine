package com.yxz.mySimpleDistributedCrawler.spider;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

/**
* @author Yu 
* 网页解析器
*/
public class Parser {

	public static List<String> parse(Page page) {
		List<String> urls = new LinkedList<>();
		String content = page.getResult();
		Document doc = Jsoup.parse(content);
		doc.select("a");
		Elements es=doc.select("a");
    	for(Iterator<Element> it = es.iterator(); it.hasNext();) {
    		Element e = it.next();
    		urls.add(e.attr("href"));
    	}
		return urls;
	}

}
