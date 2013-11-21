package com.sdata.sense.parser.facebook;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import net.sf.json.JSONArray;
import net.sf.json.JSONObject;

import com.lakeside.core.utils.StringUtils;
import com.sdata.core.Configuration;
import com.sdata.core.NegligibleException;
import com.sdata.core.RawContent;
import com.sdata.core.RunState;
import com.sdata.core.http.HttpPage;
import com.sdata.core.http.HttpPageLoader;
import com.sdata.core.parser.ParseResult;
import com.sdata.sense.Constants;
import com.sdata.sense.SenseFetchDatum;
import com.sdata.sense.SenseIDBuilder;
import com.sdata.sense.item.SenseCrawlItem;
import com.sdata.sense.parser.SenseParser;

/**
 * @author zhufb
 *
 */
public class SenseFacebookParser extends SenseParser{
	
	protected final static String DATA_KEY = "data";
	protected final static String PAGEING_KEY = "paging";
	protected final static String NEXT_KEY = "next";
	protected final static String FACEBOOK_POST_URL = "https://www.facebook.com/%s/posts/%s";
	protected final static String FACEBOOK_COMMENTS = "https://graph.facebook.com/%s/comments";
	protected final static String FACEBOOK_COMMENTS_KEY = "comments";
	
	private final static String USER_KEY = "from";
	private final static String FACEBOOK_API = "https://graph.facebook.com/";
	private final static String FACEBOOK_USER_PIC = "/picture?type=normal";
	private final static String USER_HEAD = "head";
	
	public SenseFacebookParser(Configuration conf) {
		super(conf);
	}
	
	@Override
	public ParseResult parseCrawlItem(Configuration conf,RawContent rc,SenseCrawlItem item) {
		if(rc == null){
			throw new NegligibleException("RawContent is null!");
		}
		String content = rc.getContent();
		if(StringUtils.isEmpty(content)){
			throw new NegligibleException("init web content is empty!");
		}
		if(!(content.startsWith("{")&&content.endsWith("}"))){
			throw new NegligibleException("facebook get content is not json!");
		}
		JSONObject data = JSONObject.fromObject(content);
		
		ParseResult result = new ParseResult();
		this.setCategorys(result, data);
		this.setPostDatums(result, data, item);
		return result;
	}
	
	private void setPostDatums(ParseResult result,JSONObject data,SenseCrawlItem item){
		JSONArray feeds = data.getJSONArray(DATA_KEY);
		if(feeds == null||feeds.size() == 0){
			return;
		}
		
		Iterator<JSONObject> iterator = feeds.iterator();
		while(iterator.hasNext()){
			JSONObject o = iterator.next();
			SenseFetchDatum datum = new SenseFetchDatum();
			String id = o.getString(Constants.DATA_ID);
			int index = id.indexOf("_");
			String url = String.format(FACEBOOK_POST_URL,id.substring(0,index),id.substring(index+1,id.length()));
			String pk = SenseIDBuilder.build(item, id);
			o.put(Constants.DATA_PK, pk);
			o.put(Constants.DATA_URL, url);
			datum.setUrl(url);
			datum.setId(pk);
			datum.setMetadata((Map)o);
			datum.setCurrent(item.getEntryUrl());
			datum.setCrawlItem(item);
			
			//add comments list
			this.addComments(datum,item);
			result.addFetchDatum(datum);
		}
	}
	
	private void addComments(SenseFetchDatum datum,SenseCrawlItem item){
		String url = String.format(FACEBOOK_COMMENTS,datum.getMeta(Constants.DATA_ID));
		datum.getMetadata().remove(FACEBOOK_COMMENTS_KEY);
		List<Object> comments = new ArrayList<Object>();
		while(!StringUtils.isEmpty(url)){
			HttpPage page = HttpPageLoader.getDefaultPageLoader().download(url);
			String contentHtml = page.getContentHtml();
			JSONObject data = JSONObject.fromObject(contentHtml);
			if(data.containsKey(DATA_KEY) && data.get(DATA_KEY) instanceof JSONArray){
				JSONArray array = data.getJSONArray(DATA_KEY);
				Iterator<JSONObject> iterator = array.iterator();
				while(iterator.hasNext()){
					JSONObject o = iterator.next();
					String id = o.getString(Constants.DATA_ID);
					String pk = SenseIDBuilder.build(item, id);
					o.put(Constants.DATA_PK, pk);
					comments.add(o);
				}
			}
			url = this.getNext(data);
		}
		datum.addMetadata(FACEBOOK_COMMENTS_KEY, comments);
	}
	
	private void setCategorys(ParseResult result,JSONObject data){
		String next = getNext(data);
    	if(next != null){
    		result.addCategory(next);
    	}
	}
	
	private String getNext(JSONObject data){
		if(data == null||data.isNullObject()){
			return null;
		}
		Object obj = data.get(PAGEING_KEY);
		if(obj == null ||!(obj instanceof JSONObject)){
			return null;
		}
		if(!((JSONObject)obj).containsKey(NEXT_KEY)){
			return null;
		}

		return ((JSONObject)obj).getString(NEXT_KEY);
	}
	
	public SenseFetchDatum parseDatum(SenseFetchDatum datum,Configuration conf, RawContent c) {
		Map<String, Object> feed = datum.getMetadata();
		Map user = (Map)feed.get(USER_KEY);
		String uid = StringUtils.valueOf(user.get(Constants.DATA_ID));
		feed.put(Constants.DATA_USER, this.getUserProfile(conf,uid));
		datum.setMetadata(feed);
		return datum;
	}
	
	private JSONObject getUserProfile(Configuration conf,String uid){
		String uurl = FACEBOOK_API.concat(uid);
		HttpPage upage = HttpPageLoader.getDefaultPageLoader().download(this.mergeTokenUrl(conf,uurl));
		JSONObject profile = JSONObject.fromObject(upage.getContentHtml());
		String picurl = uurl.concat(FACEBOOK_USER_PIC);
		HttpPage picpage = HttpPageLoader.getDefaultPageLoader().download(picurl);
		profile.put(USER_HEAD, picpage.getUrl());
		return profile;
	}
	
	public String mergeTokenUrl(Configuration conf,String url){
		if(!url.contains("?")){
			return url.concat("?access_token=").concat(getFacebookToken(conf)); 
		}
		if(!url.contains("access_token")){
			return url.concat("&access_token=").concat(getFacebookToken(conf));
		}
		return url;
	}
	
	private String getFacebookToken(Configuration conf){
			return conf.get("access_token");
	}

}