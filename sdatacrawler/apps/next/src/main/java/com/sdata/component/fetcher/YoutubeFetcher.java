package com.sdata.component.fetcher;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gdata.client.youtube.YouTubeService;
import com.google.gdata.data.Link;
import com.google.gdata.data.youtube.CommentFeed;
import com.google.gdata.data.youtube.UserProfileEntry;
import com.google.gdata.data.youtube.VideoFeed;
import com.google.gdata.util.ResourceNotFoundException;
import com.google.gdata.util.ServiceException;
import com.google.gdata.util.ServiceForbiddenException;
import com.lakeside.core.utils.StringUtils;
import com.mongodb.MongoException;
import com.sdata.component.data.dao.CommentsMgDao;
import com.sdata.component.data.dao.VideoMgDao;
import com.sdata.component.parser.YoutubeParser;
import com.sdata.core.Configuration;
import com.sdata.core.Constants;
import com.sdata.core.FetchDatum;
import com.sdata.core.RunState;
import com.sdata.core.fetcher.SdataFetcher;

/**
 * fetch youtube video
 * 
 * @author qmm
 *
 */
public class YoutubeFetcher extends SdataFetcher{
	
	private static final Logger log = LoggerFactory.getLogger("SdataCrawler.YoutubeFetcher");
	private Boolean isComplete = false;
	private VideoMgDao videodao = new VideoMgDao();
	private CommentsMgDao comtsdao = new CommentsMgDao();
	private YouTubeService myService;
	private String fileDir;
	private String currentFetchState;
	private String nextLink;
	private String[] feeds;
	
	private static final String crawlTime = "this_week";
	 
	/**
	 * The name of the server hosting the YouTube GDATA feeds
	 */
	public static final String YOUTUBE_GDATA_SERVER = "http://gdata.youtube.com";
	/**
	 * The prefix common to all standard feeds
	 */
	public static final String STANDARD_FEED_PREFIX = YOUTUBE_GDATA_SERVER
			+ "/feeds/api/standardfeeds/";
	
	//评分最高 说明：此供稿包含了评分最高的 YouTube 视频。
	//This feed contains the most highly rated YouTube videos.
	private static final String YOUTUBE_TYPE_TOP_RATED="top_rated";
	private static final String TOP_RATED_FEED = STANDARD_FEED_PREFIX + YOUTUBE_TYPE_TOP_RATED +"?time="+crawlTime;
	
	//收藏最多 说明：此供稿中包含的是被标为收藏的视频次数最多的视频。
	// This feed contains videos most frequently flagged as favorite videos.
	private static final String YOUTUBE_TYPE_TOP_FAVORITES = "top_favorites";
	private static final String TOP_FAVORITES_FEED = STANDARD_FEED_PREFIX + YOUTUBE_TYPE_TOP_FAVORITES +"?time="+crawlTime;
	
	//观看次数最多 说明：此供稿包含了观看次数最多的 YouTube 视频。
	// This feed actually returns the same content as the most_popular feed. As such, we recommend that you update your code to use the most_popular feed instead.
	private static final String YOUTUBE_TYPE__MOST_VIEWED = "most_viewed";
	private static final String MOST_VIEWED_FEED = STANDARD_FEED_PREFIX + YOUTUBE_TYPE__MOST_VIEWED +"?time="+crawlTime;
	
	//最受欢迎 说明：此供稿包含了最热门的 YouTube 视频。在选择这些视频时，YouTube 采用一种综合考虑了多种不同因素的算法确定总体热门程度。
	// This feed contains the most popular YouTube videos, selected using an algorithm that combines many different signals to determine overall popularity.
	private static final String YOUTUBE_TYPE_MOST_POPULAR = "most_popular";
	private static final String MOST_POPULAR_FEED = STANDARD_FEED_PREFIX + YOUTUBE_TYPE_MOST_POPULAR +"?time="+crawlTime;
	
	//最近上传 说明：此供稿包含了最新提交至 YouTube 的视频。
	//This feed contains the videos most recently submitted to YouTube.
	private static final String YOUTUBE_TYPE_MOST_RECENT = "most_recent";
	private static final String MOST_RECENT_FEED = STANDARD_FEED_PREFIX + YOUTUBE_TYPE_MOST_RECENT;
	
	//讨论最多 说明：此供稿包含了收到最多评论的 YouTube 视频。
	//This feed contains the YouTube videos that have received the most comments.
	private static final String YOUTUBE_TYPE_MOST_DISCUSSED = "most_discussed";
	private static final String MOST_DISCUSSED_FEED = STANDARD_FEED_PREFIX + YOUTUBE_TYPE_MOST_DISCUSSED +"?time="+crawlTime;
	
	//回复最多 说明：此供稿包含了收到最多视频回复的 YouTube 视频。
	// This feed contains YouTube videos that receive the most video responses.
	private static final String YOUTUBE_TYPE_MOST_RESPONDED = "most_responded";
	private static final String MOST_RESPONDED_FEED = STANDARD_FEED_PREFIX + YOUTUBE_TYPE_MOST_RESPONDED +"?time="+crawlTime;
	
	//近期精选 说明：此供稿包含了 YouTube 首页上或精选视频标签页中的近期精选视频。
	//This feed contains videos recently featured on the YouTube home page or featured videos tab.
	private static final String YOUTUBE_TYPE_RECENTLY_FEATURED = "recently_featured";
	private static final String RECENTLY_FEATURED_FEED = STANDARD_FEED_PREFIX + YOUTUBE_TYPE_RECENTLY_FEATURED;
	
	//This feed lists the YouTube videos most frequently shared on Facebook and Twitter. 
	//This feed is available as an experimental feature.
	private static final String YOUTUBE_TYPE_MOST_SHARED = "most_shared";
	private static final String MOST_SHARED_FEED = STANDARD_FEED_PREFIX + YOUTUBE_TYPE_MOST_SHARED;
	
	//This feed lists trending videos as seen on YouTube Trends, which surfaces popular videos as their popularity is increasing and also analyzes broader trends developing within the YouTube community. 
	//This feed is available as an experimental feature.
	private static final String YOUTUBE_TYPE_ON_THE_WEB = "on_the_web";
	private static final String ON_THE_WEB_FEED = STANDARD_FEED_PREFIX + YOUTUBE_TYPE_ON_THE_WEB;
	
	
	private static final Map<String,String> feedUrlMap = new HashMap<String,String>();;
	/**
	   * The prefix of the User Feeds
	   */
	  public static final String USER_FEED_PREFIX = YOUTUBE_GDATA_SERVER
	      + "/feeds/api/users/";
	
	
	public static final String youtubeVideoBaseUrl = "http://www.youtube.com/watch?v=";
	
	public YoutubeFetcher(Configuration conf,RunState state) throws UnknownHostException, MongoException {
		this.setConf(conf);
		this.setRunState(state);
		this.parser = new YoutubeParser(conf,state);
		fileDir = this.getConf("filrDir");
		String host = this.getConf("mongoHost");
		int port = this.getConfInt("mongoPort",27017);
		String dbName = this.getConf("mongoDbName");
		myService = new YouTubeService("YoutubeFetcher");
		currentFetchState = state.getCurrentFetchState();
		if(StringUtils.isNotEmpty(currentFetchState)){
			int num = Integer.valueOf(currentFetchState);
			num--;
			currentFetchState =String.valueOf(num); 
		}
		feeds = this.getConf("feeds").split(",");
		this.videodao.initilize(host, port, dbName);
		this.comtsdao.initilize(host, port, dbName);
		initFeedUrlMap();
	}

	
	/**
	 * this method will be call with multiple-thread
	 * @throws  
	 * 
	 */
	@Override
	public List<FetchDatum> fetchDatumList() {
		isComplete=false;
		List<FetchDatum> resultList = new ArrayList<FetchDatum>();
		if(StringUtils.isNotEmpty(nextLink)){
			int feednum = Integer.valueOf(currentFetchState);
			String feedname = feeds[feednum];
			resultList = this.fetchVideoFeed(myService, nextLink,feedname);
			//fetch related video each result
			fetchRelatedVideo(resultList);
		}else if(!isComplete){
			if(StringUtils.isEmpty(currentFetchState)){
				currentFetchState = "0";
			}else{
				int num = Integer.valueOf(currentFetchState);
				num++;
				currentFetchState =String.valueOf(num); 
			}
			this.state.updateCurrentFetchState(currentFetchState);
			int feednum = Integer.valueOf(currentFetchState);
			if(feednum+1>feeds.length){
				isComplete=true;
				currentFetchState ="";
				this.state.updateCurrentFetchState("");
			}else{
				String feedname = feeds[feednum];
				if(feedUrlMap.containsKey(feedname)){
					String url = feedUrlMap.get(feedname);
					resultList = this.fetchVideoFeed(myService, url,feedname);
				}else{
					throw new RuntimeException("feeds in crawl-youtube.xml write wrong.");
				}
				//fetch related video each result
				fetchRelatedVideo(resultList);
			}
		}
		return resultList;
	}


	private void fetchRelatedVideo(List<FetchDatum> resultList) {
		List<FetchDatum> relatedList = new ArrayList<FetchDatum>();
		if(resultList!=null && resultList.size()>0){
			for(FetchDatum datum : resultList){
				Map<String,Object> metadata = datum.getMetadata();
				List<Map<String, String>> linkList = (List<Map<String, String>>)metadata.get("link");
				for(Map<String, String> link:linkList){
					String rel = link.get("rel");
					if(rel.endsWith("#video.related")){
						String href = link.get("href");	
						List<FetchDatum> relateds = this.getRelatedList(myService,href);
						relatedList.addAll(relateds);
						break;
					}
				}
			}
		}
		resultList.addAll(relatedList);
	}
	
	private List<FetchDatum> getRelatedList(YouTubeService service,String href) {
		List<FetchDatum> resultList = new ArrayList<FetchDatum>();
		while(StringUtils.isNotEmpty(href)){
			List<FetchDatum> results = new ArrayList<FetchDatum>();
			try {
				VideoFeed videoFeed = null;
				int repeatNum = 0;
				while(true){
					try {
						videoFeed = service.getFeed(new URL(href),
								VideoFeed.class);
						break;
					}catch(ResourceNotFoundException e){
						log.error("get exception["+e.getMessage()+"] when crawl related,url is:"+href);
						return resultList;
					} catch (ServiceForbiddenException e) {
						if(e.getMessage().equals("Forbidden")){
							log.info("youtube crawler has be Forbidden when fetch user info, and wait 300s.");
							if(repeatNum>20){
								log.info("fetch related video is Forbidden url:["+href+"] . ");
								return resultList;
							}
							this.await(300000);
							repeatNum++;
							continue;
						}
					}
				}
				results = ((YoutubeParser)parser).parserYoutubeList(videoFeed,"related");
				resultList.addAll(results);
				Link next = videoFeed.getNextLink();
				if(next==null || StringUtils.isEmpty(next.getHref()) ){
					href = null;
				}else{
					href = next.getHref();
				}
			} catch (IOException e) {
				e.printStackTrace();
			} catch (ServiceException e) {
				e.printStackTrace();
			}catch(Exception e){
				log.error("get exception["+e.getMessage()+"] when crawl related,url is:"+href);
				e.printStackTrace();
				return resultList;
			}
		}
		return resultList;
	}


	private List<FetchDatum> fetchVideoFeed(YouTubeService service, String feedUrl,String feedName){
		List<FetchDatum> resultList = new ArrayList<FetchDatum>();
		try {
			VideoFeed videoFeed = null;
			int repeatNum = 0;
	    	while(true){
    			try {
    				videoFeed = service.getFeed(new URL(feedUrl),
    						VideoFeed.class);
    				break;
    			} catch(ResourceNotFoundException e){
					log.error("get exception["+e.getMessage()+"] when crawl related,url is:"+feedUrl);
					return resultList;
				} catch (ServiceForbiddenException e) {
    				if(e.getMessage().equals("Forbidden")){
    					log.info("youtube crawler has be Forbidden when fetch user info, and wait 300s.");
    					if(repeatNum>20){
    						log.info("fetch url:["+feedUrl+"] video is Forbidden. ");
    						return resultList;
    					}
    					this.await(300000);
    					repeatNum++;
    					continue;
    				}
    			}
    		}
			resultList = ((YoutubeParser)parser).parserYoutubeList(videoFeed,feedName);
			
			Link next = videoFeed.getNextLink();
			if(next==null || StringUtils.isEmpty(next.getHref()) ){
				nextLink = null;
			}else{
				nextLink = next.getHref();
			}
		} catch (IOException e) {
			e.printStackTrace();
		} catch (ServiceException e) {
			e.printStackTrace();
		}
		return resultList;
	}
	
	@Override
	public FetchDatum fetchDatum(FetchDatum datum) {
		Map<String, Object> metadata = datum.getMetadata();
		String id = (String)metadata.get("id");
		String url = youtubeVideoBaseUrl+id;
		metadata.put("videoUrl", url);
		datum.setUrl(url);
		metadata.put("fileType", "mp4");
		metadata.put("fileTypeNum", "18");
		metadata.put("fileDir",fileDir.concat(File.separator).concat("YouTube").concat(File.separator).concat(id.substring(0, 3).toLowerCase()));
		metadata.put("filePath", "YouTube".concat(File.separator).concat(id.substring(0, 3).toLowerCase()).concat(File.separator).concat(id+".mp4"));
		//fetch user info
		getUserInfo(metadata);
		//fetch comments
	    getComts(metadata,id); 
		return datum;
	}


	private void getUserInfo(Map<String, Object> metadata) {
		List<Map<String, String>> author = (List<Map<String, String>>)metadata.get("author");
		String uri = author.get(0).get("uri");
		String[] splits = uri.split("/");
		String userName = splits[splits.length-1];
	    try {
	    	UserProfileEntry userProfileEntry = null;
	    	int repeatNum = 0;
	    	while(true){
    			try {
    				userProfileEntry = myService.getEntry(new URL(
    					    USER_FEED_PREFIX + userName), UserProfileEntry.class);
    				break;
    			} catch (ServiceForbiddenException e) {
    				if(e.getMessage().equals("Forbidden")){
    					log.info("youtube crawler has be Forbidden when fetch user info, and wait 300s.");
    					if(repeatNum>20){
    						log.info("fetch url:["+uri+"] user's infomation is Forbidden. ");
    						return;
    					}
    					this.await(300000);
    					repeatNum++;
    					continue;
    				}
    			}
    		}
			Map<String ,Object> userMap =((YoutubeParser)parser).parserYoutubeUser(userProfileEntry);
			metadata.put(Constants.USER, userMap);
		} catch (Exception e) {
			log.error("crawler youtube userinfo["+userName+"] has something wrong:"+e.getMessage());
			return;
		}
		return;
	}


	private void getComts( Map<String, Object> metadata,String id) {
		Map<String, Object> commentsMap = (Map<String, Object>)metadata.get("comments");
	    if(commentsMap==null){
	    	return;
	    }
	    Map<String, Object> feedlinkMap = (Map<String, Object>)commentsMap.get("feedLink");
	    if(feedlinkMap==null){
	    	return;
	    }
	    String commentUrl = (String)feedlinkMap.get("href");
	    List<Map<String, Object>> commentsResultList = new ArrayList<Map<String, Object>>();
	    try {
	    	while(StringUtils.isNotEmpty(commentUrl)){
	    		CommentFeed commentFeed = null;
	    		int repeatNum = 0;
	    		while(true){
	    			try {
	    				commentFeed = myService.getFeed(new URL(commentUrl), CommentFeed.class);
	    				break;
	    			} catch (ServiceForbiddenException e) {
	    				if(e.getMessage().equals("Forbidden")){
	    					log.info("youtube crawler has be Forbidden when fetch comments, and wait 300s.");
	    					if(repeatNum>10){
	    						log.info("fetch id:["+id+"] video's comments is Forbidden. ");
	    						return;
	    					}
	    					this.await(300000);
	    					repeatNum++;
	    					continue;
	    				}
	    			}
	    		}
	    		Link next = commentFeed.getNextLink();
	    		if(next==null || next.getHref()==null){
	    			commentUrl=null;
	    		}else{
	    			commentUrl = commentFeed.getNextLink().getHref();
	    		}
	    		List<Map<String, Object>> commentsList = ((YoutubeParser)parser).parserYoutubeComments(commentFeed,id);
	    		for(Map<String, Object> commentMap:commentsList){
	    			UUID commentId = (UUID)commentMap.get(Constants.OBJECT_ID);
	    			if(comtsdao.isExists(commentId)){
	    				metadata.put(Constants.COMTS, commentsResultList);
	    				return;
	    			}
	    			commentsResultList.add(commentMap);
	    		}
	    	}
	    	
	    	metadata.put(Constants.COMTS, commentsResultList);
		} catch (Exception e) {
			log.error("crawler youtube comment has something wrong:"+e.getMessage());
			return;
		}
	}
	
	/**
	 * move to next crawl instance
	 * @param curUser 
	 */
	protected void moveNext() {
	}
	
	
	@Override
	public void datumFinish(FetchDatum datum) {
		if(datum!=null && StringUtils.isNotEmpty(datum.getCurrent())){
		}
	}

	@Override
	public boolean isComplete(){
		return isComplete;
	}
	
	private void initFeedUrlMap() {
		feedUrlMap.put(YOUTUBE_TYPE_TOP_RATED, TOP_RATED_FEED);
		feedUrlMap.put(YOUTUBE_TYPE_TOP_FAVORITES, TOP_FAVORITES_FEED);
		feedUrlMap.put(YOUTUBE_TYPE__MOST_VIEWED, MOST_VIEWED_FEED);
		feedUrlMap.put(YOUTUBE_TYPE_MOST_POPULAR, MOST_POPULAR_FEED);
		feedUrlMap.put(YOUTUBE_TYPE_MOST_RECENT, MOST_RECENT_FEED);
		feedUrlMap.put(YOUTUBE_TYPE_MOST_DISCUSSED, MOST_DISCUSSED_FEED);
		feedUrlMap.put(YOUTUBE_TYPE_MOST_RESPONDED, MOST_RESPONDED_FEED);
		feedUrlMap.put(YOUTUBE_TYPE_RECENTLY_FEATURED, RECENTLY_FEATURED_FEED);
		feedUrlMap.put(YOUTUBE_TYPE_MOST_SHARED, MOST_SHARED_FEED);
		feedUrlMap.put(YOUTUBE_TYPE_ON_THE_WEB, ON_THE_WEB_FEED);
	}
	
}
