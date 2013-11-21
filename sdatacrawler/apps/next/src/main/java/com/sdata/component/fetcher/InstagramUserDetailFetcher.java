package com.sdata.component.fetcher;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import net.sf.json.JSONArray;
import net.sf.json.JSONObject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.lakeside.core.utils.Assert;
import com.lakeside.core.utils.StringUtils;
import com.lakeside.core.utils.time.StopWatch;
import com.sdata.component.parser.InstagramParser;
import com.sdata.component.site.InstagramApi;
import com.sdata.core.Configuration;
import com.sdata.core.Constants;
import com.sdata.core.FetchDatum;
import com.sdata.core.Location;
import com.sdata.core.RawContent;
import com.sdata.core.RunState;
import com.sdata.core.TimeSpanCircularArea;
import com.sdata.core.fetcher.SdataFetcher;
import com.sdata.core.parser.ParseResult;
import com.sdata.core.util.WebPageDownloader;

public class InstagramUserDetailFetcher extends SdataFetcher {
	
	private InstagramUserDetailCircularAreaIterator iterator;
	
	private InstagramApi api;
	
	private static final Logger log = LoggerFactory.getLogger("SdataCrawler.InstagramFetcher");
	
	public InstagramUserDetailFetcher(Configuration conf,RunState state) {
		this.setConf(conf);
		this.setRunState(state);
		api = new InstagramApi(conf);
		parser = new InstagramParser(conf,state);
		iterator = new InstagramUserDetailCircularAreaIterator(conf,state);
	}
	
	@Override
	public List<FetchDatum> fetchDatumList() {
		if(iterator.haveNext()){
			TimeSpanCircularArea circularArea = iterator.getNextTimeSpanCircularArea();
		    String state = circularArea.toString();
			String queryUrl = api.getQueryUrl(circularArea);
			try {
				String content = WebPageDownloader.download(queryUrl);
				ParseResult parseList = parser.parseList(new RawContent(content));
				List<FetchDatum> fetchList = parseList.getFetchList();
				log.info("fetching page list ["+state+"],result:【"+fetchList.size()+"】");
				for(FetchDatum datum: fetchList){
					//datum.addMetadata("CurQuery", curQuery);
					datum.setCurrent(state);
				}
				return fetchList;
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		return null;
	}
	
	/**
	 * fetch user's  infomation
	 * @param id
	 * @return
	 * @author geyong
	 */
	protected HashMap<String,Object> fetchUserMeta(String uid){
		String userJson = null;
		try {
			String queryUrl = api.getUserInfoUrl(uid);
			userJson = WebPageDownloader.download(queryUrl);
		} catch (Exception e) {
			e.printStackTrace();
		}
		RawContent rc = new RawContent(userJson);
		rc.setMetadata("type", Constants.PARSER_TYPE_USER);
		ParseResult parseResult = parser.parseSingle(rc);
		HashMap<String, Object> userMap = (HashMap<String, Object>) parseResult.getMetadata();
		UUID id_s = StringUtils.getMd5UUID(uid);;
		if(userMap!=null){
			userMap.put(Constants.OBJECT_ID, id_s!=null?id_s:"");
		}
		return userMap;
		
	}
	
	/**
	 * fetch all comments
	 * @param id
	 * @return
	 * @author geyong
	 */
	protected Map<String,Object> fetchAllComments(String mediaId){
		String userJson = null;
		try {
			String queryUrl = api.getMediaCommentsUrl(mediaId);
			userJson = WebPageDownloader.download(queryUrl);
		} catch (Exception e) {
			e.printStackTrace();
		}
		ArrayList<String> userList= new ArrayList<String>();
		RawContent rc = new RawContent(userJson);
		rc.setMetadata("type", Constants.COMMENTS);
		ParseResult parseResult = parser.parseSingle(rc);
		Map<String, Object> resMap = parseResult.getMetadata();
		JSONArray comments = (JSONArray)resMap.get(Constants.COMMENTS);
		if(comments!=null){
			Iterator<JSONObject> commentIterator= comments.iterator();
			while(commentIterator.hasNext()){
				JSONObject comment = commentIterator.next();
				String userId = ((HashMap<String, Object>)comment.get("from")).get("id").toString();
				userList.add(userId);
			}
		}
		resMap.put(Constants.PARSER_TYPE_USERLIST, userList);
		return resMap;
		
	}
	
	/**
	 * fetch likes List
	 * @param id
	 * @return
	 * @author geyong
	 */
	protected Map<String,Object> fetchAllLikes(String mediaId){
		String userJson = null;
		try {
			String queryUrl = api.getMediaLikesUrl(mediaId);
			userJson = WebPageDownloader.download(queryUrl);
		} catch (Exception e) {
			e.printStackTrace();
		}
		ArrayList<String> userList= new ArrayList<String>();
		RawContent rc = new RawContent(userJson);
		rc.setMetadata("type", Constants.LIKES);
		ParseResult parseResult = parser.parseSingle(rc);
		Map<String, Object> resMap = parseResult.getMetadata();
		JSONArray likes = (JSONArray)resMap.get(Constants.LIKES);
		if(likes!=null){
			Iterator<JSONObject> likeIterator= likes.iterator();
			while(likeIterator.hasNext()){
				JSONObject like = likeIterator.next();
				String userId = String.valueOf(like.get("id"));
				userList.add(userId);
			}
		}
		resMap.put(Constants.PARSER_TYPE_USERLIST, userList);
		return resMap;
		
	}
	

	@Override
	public FetchDatum fetchDatum(FetchDatum datum) {
		StopWatch watch = new StopWatch();
		watch.start();
		Map<String, Object> media=datum.getMetadata();
		if(null!=media){
			ArrayList<HashMap<String, Object>> users = new ArrayList<HashMap<String, Object>>();
			String mediaId = String.valueOf(media.get("id"));
			//get user infor
			Map<String, Object> user =(Map<String, Object>)media.get("user");
			users.add(this.fetchUserMeta(String.valueOf(user.get("id"))));
			//get comments user infor
			Map<String, Object> comments =(Map<String, Object>)media.get("comments");
			ArrayList<HashMap<String, Object>> commentsUsers =(ArrayList<HashMap<String, Object>>)comments.get("data");
			//get all Comments
			if(Integer.parseInt(String.valueOf(comments.get("count")))==commentsUsers.size()){
				for(int m=0;m<commentsUsers.size();m++){
					HashMap<String, Object> comment = commentsUsers.get(m);
					HashMap<String, Object> cuser = (HashMap<String, Object>)comment.get("from");
					users.add(this.fetchUserMeta(String.valueOf(cuser.get("id"))));
				}
			}else{
				Map<String, Object> res = this.fetchAllComments(mediaId);
				if(res!=null){
					ArrayList<Object> cmts = (ArrayList<Object>)res.get(Constants.COMMENTS);
					comments.remove("data");
					comments.put("data", cmts);
					ArrayList<String> userList = (ArrayList<String>)res.get(Constants.PARSER_TYPE_USERLIST);
					for(int i=0;i<userList.size();i++){
						users.add(this.fetchUserMeta(userList.get(i)));
					}
				}
			}
			//get likes user infor
			Map<String, Object> likes =(Map<String, Object>)media.get("likes");
			ArrayList<HashMap<String, Object>> likesUsers =(ArrayList<HashMap<String, Object>>)likes.get("data");
			//get all likes
			if(Integer.parseInt(String.valueOf(likes.get("count")))==likesUsers.size()){
				for(int m=0;m<likesUsers.size();m++){
					HashMap<String, Object> like = likesUsers.get(m);
					users.add(this.fetchUserMeta(String.valueOf(like.get("id"))));
				}
			}else{
				Map<String, Object> res = this.fetchAllLikes(mediaId);
				if(res!=null){
					ArrayList<Object> lks = (ArrayList<Object>)res.get(Constants.LIKES);
					likes.remove("data");
					likes.put("data", lks);
					ArrayList<String> userList = (ArrayList<String>)res.get(Constants.PARSER_TYPE_USERLIST);
					for(int i=0;i<userList.size();i++){
						users.add(this.fetchUserMeta(userList.get(i)));
					}
				}
			}
			datum.addMetadata("USERS", users);
		}
		log.info("fetch time:{}",watch.getElapsedTime());
		return datum;
	}
	
	

	@Override
	protected void moveNext() {
		
	}
	
	@Override
	public boolean isComplete() {
		boolean isComplete = !iterator.haveNext();
		if(isComplete){
			iterator.reset(null);
			state.setCurrentFetchState(null);
		}
		return isComplete;
	}
}

/*
 * the iterator of the region
 */
class InstagramUserDetailCircularAreaIterator {

	private static Location southwest;
	private static Location northeast;
	
	private static BigDecimal regionLatitudeOffset = new BigDecimal("0.000924");
	private static BigDecimal regionLongitudeOffset = new BigDecimal("0.000937");

	private static TimeSpanCircularArea currentArea = null;

	private int confDistance;
	private Date confStartTime;
	private int confTimeSpan;
	public InstagramUserDetailCircularAreaIterator(Configuration conf,RunState state) {
		// ConfigSetting setting = ConfigSetting.instance(taskId);
		southwest = conf.getLocation("southwest");
		northeast = conf.getLocation("northeast");
		Assert.notNull(southwest,"the setting value of southwest location is empty!");
		Assert.notNull(northeast,"the setting value of northeast location is empty!");
		String currentFetchState = state.getCurrentFetchState();
		TimeSpanCircularArea area = TimeSpanCircularArea.convert(currentFetchState);
		confDistance = conf.getInt("Distance",100);
		regionLatitudeOffset = new BigDecimal(Double.toString((confDistance*1.414)/111133));
		regionLongitudeOffset = new BigDecimal(Double.toString((confDistance*1.414)/111319));
		confStartTime=conf.getDate("StartTime");
		confTimeSpan=conf.getInt("TimeSpan",600);
		this.init(area,conf);
	}

	public void init(String cityName) {
		if (currentArea == null) {
			currentArea = new TimeSpanCircularArea(southwest, regionLatitudeOffset,regionLongitudeOffset,confDistance,confStartTime,confTimeSpan);
		}
	}

	public void init(TimeSpanCircularArea tmsCircularArea,Configuration conf) {
		if (currentArea == null) {
			if (tmsCircularArea != null) {
				currentArea = tmsCircularArea;
			} else {
				currentArea = new TimeSpanCircularArea(southwest,regionLatitudeOffset, regionLongitudeOffset,confDistance,confStartTime,confTimeSpan);
			}
		}
	}

	/**
	 * reset to start from south west point
	 */
	public void reset(Date startTime) {
		if(null==startTime){
			startTime = confStartTime;
		}
		currentArea = new TimeSpanCircularArea(southwest,regionLatitudeOffset, regionLongitudeOffset,confDistance,startTime,confTimeSpan);
	}

	public TimeSpanCircularArea getNextTimeSpanCircularArea() {
		TimeSpanCircularArea currentTmsArea = currentArea;
		try {
			if (currentTmsArea.getMaxX().compareTo(northeast.getLongitude()) > 0) {
				Location newp = new Location(currentArea.getLoc().getLatitude(), southwest.getLongitude());
				Date startTime=currentArea.getStartTime();
				int timeSpan=currentArea.getTimeSpan();
				int distance=currentArea.getDistance();
				currentArea = new TimeSpanCircularArea(newp,regionLatitudeOffset, regionLongitudeOffset,distance,startTime,timeSpan);
				currentArea.move(regionLatitudeOffset, BigDecimal.ZERO);
			} else {
				currentArea.move(BigDecimal.ZERO, regionLongitudeOffset);
			}
			TimeSpanCircularArea curRegion = currentTmsArea.clone();
			return curRegion;
		} finally {
		}
	}

	public boolean haveNext() {
		if (currentArea.getMaxX().compareTo(northeast.getLongitude()) > 0
				&& currentArea.getMaxY().compareTo(northeast.getLatitude()) > 0) {
			if (currentArea instanceof TimeSpanCircularArea) {
				TimeSpanCircularArea tmsArea = (TimeSpanCircularArea) currentArea;
				tmsArea.moveNextTime();
				this.reset(tmsArea.getStartTime());
				return true;
			}
			return false;
		}
		return true;
	}
}
