package com.sdata.core.parser.html.util;

import java.util.Map;

import org.apache.commons.httpclient.HttpStatus;
import org.apache.commons.lang.StringUtils;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;

import com.lakeside.download.http.HttpPage;
import com.lakeside.download.http.HttpPageLoader;

/**
 * @author zhufb
 *
 */
public class Documents {

	private static HttpPageLoader advancePageLoader = HttpPageLoader.getAdvancePageLoader();
	public static Document getDocument(String url){
		if(StringUtils.isEmpty(url)){
			return null;
		}
		HttpPage page = advancePageLoader.get(url);
		if(page.getStatusCode()!=HttpStatus.SC_OK){
			return null;
		}
		return parseDocument(page.getContentHtml(),url);
	}

	public static Document getDocument(String url,Map<String,String> header){
		if(StringUtils.isEmpty(url)){
			return null;
		}
		HttpPage page = advancePageLoader.get(header,url);
		if(page.getStatusCode()!=HttpStatus.SC_OK){
			return null;
		}
		return parseDocument(page.getContentHtml(),url);
	}

	public static Document parseDocument(String content){
		if(StringUtils.isEmpty(content)){
			return null;
		}
		return Jsoup.parse(content);
	}
	
	public static Document parseDocument(String content,String baseUri){
		if(StringUtils.isEmpty(content)){
			return null;
		}
		Document doc =Jsoup.parse(content,baseUri);
		return doc;
	}
	
	public static void wait(int s){
		try {
			Thread.sleep(s*1000);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	
}
