package com.sdata.component.word.twitter;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import net.sf.json.JSONObject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import twitter4j.TwitterException;

import com.lakeside.core.utils.PatternUtils;
import com.lakeside.core.utils.StringUtils;
import com.nus.next.db.hbase.HBaseFactory;
import com.nus.next.db.hbase.thrift.HBaseClient;
import com.nus.next.db.hbase.thrift.HBaseClientFactory;
import com.sdata.component.site.TwitterApi;
import com.sdata.component.word.WordParser;
import com.sdata.core.Configuration;
import com.sdata.core.FetchDatum;
import com.sdata.core.http.HttpPage;
import com.sdata.core.http.HttpPageLoader;
import com.sdata.core.item.CrawlItem;

public class TwitterWordParser extends WordParser {

	protected static final Logger log = LoggerFactory
			.getLogger("SdataCrawler.TwitterWordParser");
	private TwitterApi api = new TwitterApi();
	private final String baseUrl = "https://twitter.com/";
	private static String URL = "https://twitter.com/i/search/timeline?type=recent&src=typd&include_available_features=1&include_entities=1&q=%s&max_id=%s";
	private boolean complete = false;
	private String currentId = "";
//	private HBaseDao hbaseDao;
	private HBaseClient client;
	
	
	public TwitterWordParser(Configuration conf) {
		String consumerKey = conf.get("ConsumerKey");
		String consumerSecret = conf.get("ConsumerSecret");
		String AccessToken = conf.get("AccessToken");
		String AccessTokenSecret = conf.get("AccessTokenSecret");
		api.setOAuth(consumerKey, consumerSecret, AccessToken, AccessTokenSecret);
//		hbaseDao = new HBaseDao("next", HBaseFactory.getNewestDBPool());
		client = HBaseClientFactory.getClientWithNormalSeri("next"); 
	}

	@Override
	public List<FetchDatum> getFetchList(CrawlItem item, Date startTime,
			Date endTime, String currentState) {
		complete = false;
		if(StringUtils.isEmpty(currentState)){
			currentId = "0";
		}else{
			currentId = currentState;
		}
		List<FetchDatum> list = new ArrayList<FetchDatum>();
		String url = String.format(URL,item.getKeyword(),currentId);
		JSONObject json = fetchPage(url);
		String[] statusIds = getStatusIds(json);
		Arrays.sort(statusIds,new IDComparator());
		for(String s:statusIds){
			if(exists(s)){
				continue;
			}
			Long id = Long.valueOf(s);
			try {
				Map<String,Object> status = api.getStatus(id);
				this.putUrl(status);
				FetchDatum datum = new FetchDatum();
				status.put("dtf_w", item.getKeyword());
				status.put("fet_time", System.currentTimeMillis());
				datum.setMetadata(status);
				datum.setId(s);
				list.add(datum);
				super.await(10*1000);
			} catch (TwitterException e) {
				e.printStackTrace();
			}
		}
		if(!hasMore(json)){
			complete = true;
		}
		currentId = statusIds[0];
		return list;
	}

	@Override
	public boolean isComplete() {
		return complete;
	}

	private void putUrl(Map<String,Object> status){
		Map user = (Map)status.get("user");
		Long uid = Long.valueOf(StringUtils.valueOf(user.get("id")));
		String name = StringUtils.valueOf(user.get("name"));
		Object sname = user.get("screen_name");
		String uurl = baseUrl+sname;
		String statusUrl = baseUrl+sname+"/status/"+status.get("id");
		String prfiu = StringUtils.valueOf(user.get("profile_image_url"));
		status.put("url", statusUrl);
		status.put("uurl", uurl);
		status.put("sname", name);
		status.put("name", sname);
		status.put("head", prfiu);
	}
	@Override
	public FetchDatum getFetchDatum(FetchDatum datum) {
		return datum;
	}

	@Override
	public String getCurrentState() {
		return currentId;
	}
	

	private boolean hasMore(JSONObject json) {
		return json.getBoolean("has_more_items");
	}
	
	private boolean exists(String id){
		return client.exists("word_twitter_tweets", id);
	}
	/**
	 * 
	 * fetch next page
	 * 
	 * @param id
	 * @return
	 */
	private JSONObject fetchPage(String url) {
		HttpPage page = HttpPageLoader.getDefaultPageLoader().download(url);
		String content = page.getContentHtml();
		JSONObject json = JSONObject.fromObject(content);
		return json;
	}
	
	private String[] getStatusIds(JSONObject json) {
		if(!json.containsKey("items_html")){
			return null;
		}
		String html = json.getString("items_html");
		return getAllMatchPattern("data-tweet-id=\"(\\d*)\"", html);
	}
	
	private String[] getAllMatchPattern(String regex,String input){
		Pattern pat = PatternUtils.getPattern(regex);
		Matcher matcher = pat.matcher(input);
		ArrayList result = new ArrayList();
		while(matcher.find()){
			for(int i=0;i<matcher.groupCount();i++){
				result.add(matcher.group(i+1));
			}
		}
		return (String[]) result.toArray(new String[result.size()]);
	}
	
	/**
	 * class web data comparator
	 * 
	 * @author zhufb
	 *
	 */
	class IDComparator<String> implements Comparator{
		 public int compare(Object arg0, Object arg1) {
			 Long a0 = Long.valueOf(arg0.toString());
			 Long a1 = Long.valueOf(arg1.toString());
			 return a0.compareTo(a1);
		 }
	}

}
