package com.sdata.component.word.weibo;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import weibo4j.Timeline;
import weibo4j.Weibo;
import weibo4j.WeiboHelper;
import weibo4j.model.WeiboException;
import weibo4j.util.WeiboServer;

import com.framework.db.hbase.thrift.HBaseClient;
import com.framework.db.hbase.thrift.HBaseClientFactory;
import com.lakeside.core.utils.PatternUtils;
import com.lakeside.core.utils.StringUtils;
import com.lakeside.core.utils.time.DateTimeUtils;
import com.lakeside.download.http.HttpPage;
import com.lakeside.download.http.HttpPageLoader;
import com.sdata.component.word.WordParser;
import com.sdata.core.Configuration;
import com.sdata.core.CrawlAppContext;
import com.sdata.core.FetchDatum;
import com.sdata.core.item.CrawlItem;

/**
 * @author zhufb
 *
 */
public class WeiboWordParser extends WordParser {

	protected static final Logger log = LoggerFactory
			.getLogger("SdataCrawler.WeiboWordParser");
	private Timeline tm = new Timeline() ;
	private String host = "http://www.weibo.com/";
	private static String URL = "http://s.weibo.com/weibo/%s&xsort=time&timescope=custom:%s:%s&page=%d";
	private boolean complete = false;
	private int page;
	private static HBaseClient client = HBaseClientFactory.getClientWithNormalSeri("next"); 
	Map<String,String> header = new HashMap<String,String>();
	public WeiboWordParser(Configuration conf) {
		WeiboServer.init(CrawlAppContext.state.getCrawlName());
		header.put("Cookie", Weibo.getCookie());
	}

	@Override
	public List<FetchDatum> getFetchList(CrawlItem item, Date startTime,
			Date endTime, String currentState) {
		complete = false;
		if(StringUtils.isEmpty(currentState)){
			page = 1;
		}else{
			page = Integer.parseInt(currentState);
		}
		List<FetchDatum> list = new ArrayList<FetchDatum>();
		String url = String.format(URL,item.getKeyword(),DateTimeUtils.format(startTime, "yyyy-MM-dd-HH"),DateTimeUtils.format(endTime, "yyyy-MM-dd-HH"),page);
		List<String> ids = new ArrayList<String>();
		boolean have = getStatusIds(ids,fetchPage(url));
		if(!have ||ids==null||ids.size() == 0){
			complete = true;
			return list;
		}
		for(String s:ids){
			try{
				if(exists(s)){
					continue;
				}
				FetchDatum datum = new FetchDatum();
				datum.addMetadata("dtf_w", item.getKeyword());
				datum.setId(s);
				list.add(datum);
			}catch(WeiboException e){
				System.out.println(s+" failed ");
				e.printStackTrace();
			}
		}
		if(++page>50){
			complete = true;
		}
		return list;
	}

	@Override
	public boolean isComplete() {
		return complete;
	}

	@Override
	public FetchDatum getFetchDatum(FetchDatum datum) {
		super.await(12*1000);
		Map<String,Object> status = tm.showStatusJson(datum.getId().toString());
		String tweetUrl = this.getTweetUrl(status);
		status.put("url", tweetUrl);
		status.put("fet_time", System.currentTimeMillis());
		datum.setMetadata(status);
		return datum;
	}

	@Override
	public String getCurrentState() {
		return String.valueOf(page);
	}
	
	private boolean exists(String id){
		return client.exists("word_weibo_tweets", id);
	}
	/**
	 * 
	 * fetch next page
	 * 
	 * @param id
	 * @return
	 */
	private String fetchPage(String url) {
		super.await(25*1000);
		HttpPage page = HttpPageLoader.getDefaultPageLoader().download(header,url);
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

	private String getTweetUrl(Map<String,Object> matadata){
		StringBuffer sb = new StringBuffer(host);
		Map user = (Map)matadata.get("user");
		if(user == null||user.size() == 0){
			return null;
		}
		Object id = matadata.get("id");
		if(id == null||!StringUtils.isNum(id.toString())){
			return null;
		}
		sb.append(user.get("id")).append("/").append(WeiboHelper.id2Mid(Long.valueOf(id.toString())));
		return sb.toString();
	}
	
}
