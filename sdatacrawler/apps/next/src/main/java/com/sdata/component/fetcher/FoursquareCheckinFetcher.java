package com.sdata.component.fetcher;

import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.bson.types.ObjectId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.lakeside.core.utils.PatternUtils;
import com.lakeside.core.utils.StringUtils;
import com.sdata.component.parser.FoursquareCheckinParser;
import com.sdata.component.site.TwitterApi;
import com.sdata.core.Configuration;
import com.sdata.core.Constants;
import com.sdata.core.FetchDatum;
import com.sdata.core.RawContent;
import com.sdata.core.RunState;
import com.sdata.core.fetcher.SdataFetcher;
import com.sdata.core.parser.ParseResult;
import com.sdata.core.util.WebPageDownloader;

/**
 * foursquare check in data crawler.
 * 
 * this crawler is base on twitter search api, it first will search foursquare check in tweets from twitter service,
 * then fetch foursqaure information base on the tweet information.
 * 
 * @author houdejun
 *
 */
public class FoursquareCheckinFetcher extends SdataFetcher {

	private static final Logger log = LoggerFactory.getLogger("SdataCrawler.FoursquareCheckinFetcher");
	
	private final TwitterApi api = new TwitterApi();
	private static final String FOUR_SQ_KEY_WORDS = "4sq.com";
	private long nextSinceId = -1;
	
	private FoursquareCheckinParser parser ;
	
	private boolean iscomplete = false;
	
	private String nextPageUrl="";
	private String lastFirstAndSecondIds="";
	
	public FoursquareCheckinFetcher(Configuration conf,RunState state) {
		this.setConf(conf);
		this.setRunState(state);
		String currentState = state.getCurrentFetchState();
		if(StringUtils.isNum(currentState)){
			nextSinceId = Long.valueOf(currentState);
		}else{
			if("null".equals(currentState)){
				currentState="";
			}
			nextPageUrl = currentState;
		}
		this.parser = new FoursquareCheckinParser(conf,state);
	}

	@Override
	public List<FetchDatum> fetchDatumList() {
		String content = null;
		String currentUrl=nextPageUrl;
		long curSinceId = nextSinceId;
		String currentState="";
		if(StringUtils.isEmpty(nextPageUrl)){
			content = api.search(FOUR_SQ_KEY_WORDS,1,nextSinceId);
			currentState = StringUtils.valueOf(nextSinceId);
		}else{
			content = api.searchByUrl(nextPageUrl);
			currentState = nextPageUrl;
		}
		if(StringUtils.isEmpty(content)){
			return null;
		}
		RawContent c = new RawContent(content);
		c.setMetadata("currentState", currentState);
		ParseResult result = parser.parseList(c);
		int size=0;
		String firstAndSecondIds = "";
		if(result != null){
			Long _maxId = result.getLong("max_id");
			if(_maxId!=null)
				nextSinceId = _maxId;
			nextPageUrl = result.getString("next_page");
			size= result.getListSize();
			firstAndSecondIds = getFirstAndSecondIds(result.getFetchList());
		}
		// last page or begin to returning duplication page content
		if(size==0 || (StringUtils.isNotEmpty(lastFirstAndSecondIds) && lastFirstAndSecondIds.equals(firstAndSecondIds))){
			nextPageUrl="";
			lastFirstAndSecondIds = "";
			log.info("search foursquare tweets from twitter reach to the last page ");
			return null;
		}
		lastFirstAndSecondIds = firstAndSecondIds;
		// for logger
		if(StringUtils.isEmpty(currentUrl)){
			log.info("search foursquare tweets from twitter, count [{}] since [{}]",size,curSinceId);
		}else{
			log.info("search foursquare tweets from twitter, count [{}] "+currentUrl,size);
		}
		return result.getFetchList();
	}

	@Override
	public FetchDatum fetchDatum(FetchDatum datum) {
		// this is the tweet text content, be used to extract the foursquare address 
		String text = datum.getMeta("text");
		String url = PatternUtils.getMatchPattern("(http://[a-zA-Z0-9/\\.]*)", text, 1);
		if(StringUtils.isNotEmpty(url)){
			String content = WebPageDownloader.download(url);
			datum.setUrl(url);
			url = PatternUtils.getMatchPattern("URL=(http://[a-zA-Z0-9/\\.]*)", content, 1);
			url=WebPageDownloader.getRedirectUrl(url);
			Boolean isCheckIn = PatternUtils.find("/checkin/.*",url);
			if(!isCheckIn){
				datum.clearMetadata();
				return datum;
			}
			content=WebPageDownloader.download(url);
			//datum.setUrl(url);
			RawContent c = new RawContent(url,content);
			c.setMetadata("text", text);
			ParseResult result = parser.parseSingle(c);
			if(result!=null){
				datum.addAllMetadata(result.getMetadata());
			}
		}
		
		Map<String, Object> metadata = datum.getMetadata();
		metadata.put(Constants.FETCH_TIME, new Date());
		Boolean isCheckIn = (Boolean)metadata.remove("isCheckIn");
		if(!isCheckIn  || (!metadata.containsKey("checkinId") && !metadata.containsKey("id"))){
			datum.clearMetadata();
			return datum;
		}
		// 如果是checkinid,将checkinid转换成id
		if(metadata.containsKey("checkinId")){
			metadata.put("id", metadata.get("checkinId"));
		}
		// 将id转换成objectid类型，由于foursquare的id本身是objectid类型。
		if(metadata.containsKey("id")){
			Object id = getObjectId(metadata.get("id"));
			metadata.put("id", id);
		}
		
		Map<String, Object> user = (Map<String, Object>) metadata.get("user");
		if(user!=null){
			Object userId = this.ensureObjectID(user, "id");
			metadata.put("usrid",userId);
		}
		
		Object _venue = metadata.remove("venue");
		if(_venue!=null && _venue instanceof Map){
			Map<String, Object> venue = new HashMap<String, Object>();
			venue.putAll((Map<String, Object>)_venue);
			String id = StringUtils.valueOf(venue.get("id"));
			String name = StringUtils.valueOf(venue.get("name"));
			venue.put("oid", StringUtils.chompHeader(id, "v"));
			Object venueId = this.ensureObjectID(venue, "id");
			venue.put("id", venueId);
			metadata.put("venue", venue);
			metadata.put("vid",venueId);
			metadata.put("lng",getEmbedMapValue(venue,"location","lng"));
			metadata.put("lat",getEmbedMapValue(venue,"location","lat"));
			metadata.put("docloc",getEmbedMapValue(venue,"location","country"));
			metadata.put("vname",name);
		}
		return datum;
	}
	
	
	private Object getEmbedMapValue(Map<String,?> map,String firstKey,String secKey){
		if(map==null){
			return null;
		}
		if(map.containsKey(firstKey)){
			map = (Map<String,?>)map.get(firstKey);
			if(map!=null){
				return map.get(secKey);
			}
		}
		return null;
	}
	
	private Object ensureObjectID(Map<String, Object> meta,String key){
		Object id = meta.get(Constants.OBJECT_ID);
		if(id ==null ){
			id = meta.get(key);
			id = getObjectId(id);
			return id;
		}
		return id;
	}
	
	private Object getObjectId(Object id) {
		if(id ==null ){
			throw new RuntimeException("the property of id is null!");
		}
		String strId = StringUtils.valueOf(id);
		if("".equals(strId))
			throw new RuntimeException("the property of id is empty!");
		if(ObjectId.isValid(strId)){
			id = new ObjectId(strId);
		}else if(StringUtils.isNum(strId)){
			id = Long.valueOf(strId);
		}else {
			throw new RuntimeException("the property of id is a invalid string");
		}
		return id;
	}

	@Override
	public boolean isComplete() {
		return iscomplete;
	}
	
	private String getFirstAndSecondIds(List<FetchDatum> list){
		StringBuilder ids = new StringBuilder();
		if(list!=null && list.size()>0){
			ids.append(list.get(0).getMeta("twitterid"));
			if(list.size()>1)ids.append(","+list.get(1).getMeta("twitterid"));
		}
		return ids.toString();
	}
}
