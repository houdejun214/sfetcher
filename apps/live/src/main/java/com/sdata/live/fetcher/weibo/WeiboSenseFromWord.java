package com.sdata.live.fetcher.weibo;

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
import com.sdata.context.config.Configuration;
import com.sdata.core.FetchDatum;
import com.sdata.core.item.CrawlItemEnum;
import com.sdata.core.parser.html.util.DocumentUtils;
import com.sdata.proxy.SenseFetchDatum;
import com.sdata.proxy.item.SenseCrawlItem;

/**
 * @author zhufb
 *
 */
public class WeiboSenseFromWord extends WeiboSenseFrom {
	private final String search = "http://s.weibo.com/weibo/%s&xsort=time&page=%d";
	private Timeline tm = new Timeline() ;
	private int maxPage ;
	public WeiboSenseFromWord(Configuration conf){
		this.maxPage = conf.getInt("sense.weibo.html.maxpage", 50);
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
		while(true){
			HttpPage page = advancePageLoader.download(httpHeader,url);
			String contentHtml = page.getContentHtml();
			if(isValid(contentHtml)){
				return contentHtml;
			}
			super.refreshHeader();
		}
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
	
	@Override
	public SenseFetchDatum getDatum(SenseFetchDatum datum) {
		try{
			Map<String,Object> status = tm.showStatusJson(datum.getId().toString());
			String tweetUrl = getTweetUrl(status);
			status.put("url", tweetUrl);
			datum.setMetadata(JSONUtils.map2JSONObj(status));
			datum.setUrl(tweetUrl);
			return datum;
		}catch(WeiboException e){
		}
		return null;
	}
}
