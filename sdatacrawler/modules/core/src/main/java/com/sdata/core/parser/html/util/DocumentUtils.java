package com.sdata.core.parser.html.util;

import java.util.Map;

import org.apache.commons.httpclient.HttpStatus;
import org.apache.commons.lang.StringUtils;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;

import com.sdata.core.http.HttpPage;
import com.sdata.core.http.HttpPageLoader;

/**
 * @author zhufb
 *
 */
public class DocumentUtils {
	
	public static Document getDocument(String url){
		if(StringUtils.isEmpty(url)){
			return null;
		}
		HttpPage page = HttpPageLoader.getDefaultPageLoader().download(url);
		if(page.getStatusCode()!=HttpStatus.SC_OK){
			return null;
		}
		return parseDocument(page.getContentHtml(),url);
	}

	public static Document getDocument(String url,Map<String,String> header){
		if(StringUtils.isEmpty(url)){
			return null;
		}
		HttpPage page = HttpPageLoader.getDefaultPageLoader().download(header,url);
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
