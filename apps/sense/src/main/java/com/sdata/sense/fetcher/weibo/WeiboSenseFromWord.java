package com.sdata.sense.fetcher.weibo;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.jsoup.nodes.Document;

import weibo4j.Timeline;
import weibo4j.model.WeiboException;

import com.lakeside.core.utils.JSONUtils;
import com.lakeside.core.utils.PatternUtils;
import com.lakeside.core.utils.StringUtils;
import com.lakeside.core.utils.UrlUtils;
import com.lakeside.download.http.HttpPage;
import com.lakeside.download.http.HttpPageLoader;
import com.sdata.core.Configuration;
import com.sdata.core.FetchDatum;
import com.sdata.core.item.CrawlItemEnum;
import com.sdata.core.parser.html.util.DocumentUtils;
import com.sdata.sense.SenseFetchDatum;
import com.sdata.sense.item.SenseCrawlItem;

/**
 * @author zhufb
 *
 */
public class WeiboSenseFromWord extends WeiboSenseFrom {
	private final String search = "http://s.weibo.com/weibo/%s&xsort=time&page=%d";
	private Configuration conf;
	private Timeline tm = new Timeline() ;
	private Map<String,String> httpHeader;
	private int maxPage ;
	private int htmlWait ;
	private int apiWait ;
	public WeiboSenseFromWord(Configuration conf,Map<String,String> httpHeader){
		this.conf = conf;
		this.httpHeader = httpHeader;
		this.maxPage = conf.getInt("sense.weibo.html.maxpage", 50);
		this.htmlWait = conf.getInt("sense.weibo.html.wait.seconds", 25);
		this.apiWait = conf.getInt("sense.weibo.api.wait.secnonds", 12);
	}
	
	public List<FetchDatum> getData(SenseCrawlItem item) {
		List<FetchDatum> result = new ArrayList<FetchDatum>();
		String from = StringUtils.valueOf(item.getParam(CrawlItemEnum.KEYWORD.getName()));
		if(StringUtils.isEmpty(from)){
			return result;
		}
		List<String> ids = new ArrayList<String>();
		for(int page = 1;page <=maxPage;page++){
			String url = String.format(search, UrlUtils.encode(from),page);
			boolean have = getStatusIds(ids,fetchPage(url));
			if(!have){
				break;
			}
		}
		result.addAll(this.parseDatum(ids, item));
		return result;
	}
	
	private String fetchPage(String url) {
		super.sleep(htmlWait);
		HttpPage page = HttpPageLoader.getAdvancePageLoader().download(httpHeader,url);
		return page.getContentHtml();
	}
	
	private boolean getStatusIds(List<String> result,String html) {
		if(StringUtils.isEmpty(html)){
			html = "";
		}
		return getAllMatchPattern(result,"&mid=(\\d*)", html);
	}
	
	private boolean getAllMatchPattern(List<String> result,String regex,String input){
		boolean have = false;
		Pattern pat = PatternUtils.getPattern(regex);
		Matcher matcher = pat.matcher(input);
		while(matcher.find()){
			for(int i=0;i<matcher.groupCount();i++){
				String id = matcher.group(i+1);
				if(!result.contains(id)){
					result.add(id);
				}
			}
			have = true;
		}
		return have;
	}
		
	private List<FetchDatum> parseDatum(List<String> list,SenseCrawlItem item){
		List<FetchDatum> result = new ArrayList<FetchDatum>();
		for(String s:list){
			SenseFetchDatum datum = new SenseFetchDatum();
			datum.setId(s);
			datum.setCrawlItem(item);
			result.add(datum);
		}
		return result;
	}
	
	private boolean checkDoc(Document doc){
		if(doc == null){
			return false;
		}
		if(doc.toString().contains("你的行为有些异常，请输入验证码")){
			System.out.println("你的行为有些异常，请输入验证码!");
			DocumentUtils.wait(300);
			return false;
		}
		return true;
	}

	@Override
	public SenseFetchDatum getDatum(SenseFetchDatum datum) {
//		super.sleep(apiWait);
//		int time = 0;
//		while(time<3){
			try{
				Map<String,Object> status = tm.showStatusJson(datum.getId().toString());
				String tweetUrl = getTweetUrl(status);
				status.put("url", tweetUrl);
				datum.setMetadata(JSONUtils.map2JSONObj(status));
				datum.setUrl(tweetUrl);
				return datum;
			}catch(WeiboException e){
//				time++;
//				continue;
			}
//		}
		return null;
	}
}
