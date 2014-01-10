package com.sdata.live.fetcher.weibo;

import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import weibo4j.Timeline;
import weibo4j.model.WeiboException;

import com.lakeside.core.utils.JSONUtils;
import com.lakeside.core.utils.PatternUtils;
import com.lakeside.core.utils.StringUtils;
import com.lakeside.core.utils.UrlUtils;
import com.lakeside.core.utils.time.DateTimeUtils;
import com.lakeside.download.http.HttpPage;
import com.sdata.context.config.Configuration;
import com.sdata.core.FetchDatum;
import com.sdata.core.item.CrawlItemEnum;
import com.sdata.core.parser.html.util.DocumentUtils;
import com.sdata.live.state.LiveState;
import com.sdata.proxy.SenseFetchDatum;
import com.sdata.proxy.item.SenseCrawlItem;

/**
 * @author zhufb
 *
 */
public class LiveWeiboFromWord extends LiveWeiboBase {
	//http://s.weibo.com/wb/keywod&xsort=time&scope=ori&timescope=custom:2013-12-17-0:2013-12-17-1&Refer=g
	private final String search = "http://s.weibo.com/weibo/{0}&xsort=time&scope=ori&timescope=custom:{1}:{2}&page={3}";
	private Timeline tm = new Timeline() ;
	private int maxPage ;
	private boolean complete = false; // this time range is complete or not 
	
	public LiveWeiboFromWord(Configuration conf){
		this.maxPage = conf.getInt("live.weibo.maxpage", 50);
	}
	
	@Override
	public List<FetchDatum> getList(SenseCrawlItem item, LiveState state) {
		complete = false;
		List<FetchDatum> result = new ArrayList<FetchDatum>();
		String keyword =  StringUtils.valueOf(item.getParam(CrawlItemEnum.KEYWORD.getName()));
		String starttime =DateTimeUtils.format(state.getStart(), "yyyy-MM-dd-H");// this.getUnixTime(state.getStart());
		String endtime = DateTimeUtils.format(state.getEnd(), "yyyy-MM-dd-H");//this.getUnixTime(state.getEnd());
		int page = state.getPage();
		if(StringUtils.isEmpty(keyword)){
			complete = true;
			return result;
		}
		if(page > maxPage){
			complete = true;
			return result;
		}
		keyword = UrlUtils.encode(keyword);
		List<String> ids = new ArrayList<String>();
		String url = MessageFormat.format(search,keyword ,starttime,endtime,page);
		String fetchPage = fetchPage(url);
		if(StringUtils.isEmpty(fetchPage)||fetchPage.contains("noresult_tit")){
			complete = true;
			return result;
		}
		boolean have = getStatusIds(ids,fetchPage);
		if(!have){
			complete = true;
			return result;
		}
		result.addAll(this.parseDatum(ids, item));
		return result;
	}
	
	private String fetchPage(String url) {
		while(true){
			DocumentUtils.wait(18);
			HttpPage page = advancePageLoader.download(httpHeader,url);
			String contentHtml = page.getContentHtml();
			if(isValid(contentHtml)){
				return contentHtml;
			}
			super.refreshResource();
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

	@Override
	public void next(LiveState state) {
		state.setPage(state.getPage()+1);
	}

	@Override
	public boolean isComplete() {
		return complete;
	}

}
