package com.yxz.mySimpleDistributedCrawler.utils;

import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

/**
 *  @author Yu   
 */
public class ParserUtil {

	public static Set<String> parseUrl(String html) {
		Set<String> urlSet = new HashSet<>();
		Document doc = Jsoup.parse(html);
		doc.select("a");
		Elements es=doc.select("a");
    	for(Iterator<Element> it = es.iterator(); it.hasNext();) {
    		Element e = it.next();
    		urlSet.add(e.attr("href"));
    	}
    	return urlSet;
	}
	
	//定义script的正则表达式{或<script[^>]*?>[\\s\\S]*?<\\/script>  
	private static final String scriptReg = "<[\\s]*?script[^>]*?>[\\s\\S]*?<[\\s]*?\\/[\\s]*?script[\\s]*?>";
	//定义style的正则表达式{或<style[^>]*?>[\\s\\S]*?<\\/style>  
    private static final String styleReg = "<[\\s]*?style[^>]*?>[\\s\\S]*?<[\\s]*?\\/[\\s]*?style[\\s]*?>";   
    // 定义HTML标签的正则表达式
    private static final String htmlReg = "<[^>]+>"; 
    //定义空格回车换行符
    private static final String spaceReg = "\\s*|\t|\r|\n";
    
	public static String removeHtmlTags(String html) {
		if(html == null)
			return null; 
		Pattern scriptPattern = Pattern.compile(scriptReg, Pattern.CASE_INSENSITIVE);
		Matcher scriptMatcher = scriptPattern.matcher(html);
		html = scriptMatcher.replaceAll("");
		
		Pattern stylePattern = Pattern.compile(styleReg, Pattern.CASE_INSENSITIVE);
		Matcher styleMatcher = stylePattern.matcher(html);
		html = scriptMatcher.replaceAll("");
		
		Pattern htmlPattern = Pattern.compile(htmlReg, Pattern.CASE_INSENSITIVE);
		Matcher htmlMatcher = htmlPattern.matcher(html);
		html = htmlMatcher.replaceAll("");
		
//		Pattern spacePattern = Pattern.compile(spaceReg, Pattern.CASE_INSENSITIVE);
//		Matcher spaceMatcher = spacePattern.matcher(html);
//		html = spaceMatcher.replaceAll(" ");
		
		html = html.replaceAll("&nbsp;", "");
//      html = html.substring(0, html.indexOf("。")+1);
        return html;
	}
	
	public static void main(String[] args) {
//		String clearHtml = removeHtmlTags("<div style='text-align:center;'> 整治“四风”   清弊除垢<br/><span style='font-size:14px;'> </span><span style='font-size:18px;'>公司召开党的群众路线教育实践活动动员大会</span><br/></div>");
//		System.out.println(clearHtml);
		String html = FetcherUtil.fetchHtml("www.qq.com");
//		System.out.println(html);
		Set<String> urlSet = ParserUtil.parseUrl(html);
		for(String url : urlSet) {
			System.out.println(url);
		}
		System.out.println(urlSet.size());
	}

}
