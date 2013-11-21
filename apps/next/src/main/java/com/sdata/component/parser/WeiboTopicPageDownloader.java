package com.sdata.component.parser;


import java.util.HashMap;
import java.util.Map;

import net.sf.json.JSONObject;

import org.apache.commons.httpclient.HttpStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.sdata.core.http.HttpPage;
import com.sdata.core.http.HttpPageLoader;

/**
 * sina weibo download
 * @author zhufb
 *
 */
public class WeiboTopicPageDownloader {
	private static Logger log = LoggerFactory.getLogger("SdataCrawler.WeiboTopicPageDownloader");
	public static String download(String url){
    	Map<String,String> header = new  HashMap<String,String>();
    	header.put("X-Requested-With", "XMLHttpRequest");
		try {
			while (true) {
				sleep(8);// 延迟秒
				HttpPage page = HttpPageLoader.getDefaultPageLoader().download(header,url);
				if (page.getStatusCode() != HttpStatus.SC_OK) {
					return null;
				}
				JSONObject fromObject = JSONObject.fromObject(page.getContentHtml());
				JSONObject object = (JSONObject) fromObject.get("data");
				String str = (String) object.get("html");
				if (str.contains("你刷新太快啦，休息一下吧")) {
					log.info("*********访问频率太快，正在等待，10分钟后继续访问...");
					sleep(600);
					continue;
				}
				return str;
			}
		}catch(Exception e){
			return null;
		}
	}
	
	private static void sleep(int s){
		try {
			Thread.sleep(s*1000);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
