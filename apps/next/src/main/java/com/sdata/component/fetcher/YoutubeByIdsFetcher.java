package com.sdata.component.fetcher;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;
import java.net.UnknownHostException;
import java.util.ArrayList;
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
import com.lakeside.core.utils.UUIDUtils;
import com.mongodb.MongoException;
import com.sdata.component.data.dao.CommentsMgDao;
import com.sdata.component.data.dao.VideoMgDao;
import com.sdata.component.parser.YoutubeParser;
import com.sdata.core.Configuration;
import com.sdata.core.Constants;
import com.sdata.core.FetchDatum;
import com.sdata.core.RunState;
import com.sdata.core.fetcher.SdataFetcher;
import com.sdata.core.util.ApplicationResourceUtils;

/**
 * fetch youtube video
 * 
 * @author qmm
 *
 */
public class YoutubeByIdsFetcher extends SdataFetcher{
	private static final Logger log = LoggerFactory.getLogger("SdataCrawler.YoutubeByIdsFetcher");
	
	private VideoMgDao videodao = new VideoMgDao();
	
	private CommentsMgDao comtsdao = new CommentsMgDao();
	private YouTubeService myService;
	private String fileDir;
	 
	/**
	 * The name of the server hosting the YouTube GDATA feeds
	 */
	private static final String YOUTUBE_GDATA_SERVER = "http://gdata.youtube.com";
	
	private static final String VideoApiUrl = "https://gdata.youtube.com/feeds/api/videos";
	
	/**
	 * The prefix of the User Feeds
	 */
	private static final String USER_FEED_PREFIX = YOUTUBE_GDATA_SERVER + "/feeds/api/users/";
	
	private static final String youtubeVideoBaseUrl = "http://www.youtube.com/watch?v=";
	
	private static final String feedName = "byid";
	
	private List<String> youtubeIds = new ArrayList<String>(1000);
	
	private int youtubeIdLen = 0;
	
	private int index = 0;
	
	private String youtubeIdsFile;
	
	public YoutubeByIdsFetcher(Configuration conf,RunState state) throws UnknownHostException, MongoException {
		this.setConf(conf);
		this.setRunState(state);
		this.parser = new YoutubeParser(conf,state);
		fileDir = this.getConf("filrDir");
		String host = this.getConf("mongoHost");
		int port = this.getConfInt("mongoPort",27017);
		String dbName = this.getConf("mongoDbName");
		myService = new YouTubeService("YoutubeFetcher");
		String currentFetchState = state.getCurrentFetchState();
		if(StringUtils.isNotEmpty(currentFetchState)){
			index = Integer.valueOf(currentFetchState);
		}
		this.comtsdao.initilize(host, port, dbName);
		this.videodao.initilize(host, port, dbName);
		youtubeIdsFile = this.getConf("YoutubeIdsFile");
	}
	
	/**
	 * 
	 * initialize the task 
	 * 
	 */
	@Override
	public void taskInitialize() {
		// load the id list;
		String path = ApplicationResourceUtils.getResourceUrl(youtubeIdsFile);
		BufferedReader reader = null;
		try {
			reader = new BufferedReader(new InputStreamReader(new FileInputStream(path), "utf-8"));
			String line = null;
			while ((line = reader.readLine()) != null) {
				if (StringUtils.isNotEmpty(line)) {
					youtubeIds.add(line.trim());
				}
			}
			youtubeIdLen = youtubeIds.size();
		} catch (Exception e) {
			throw new RuntimeException(e);
		} finally {
			try {
				if (reader != null) {
					reader.close();
				}
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}
	
	/**
	 * 从队列中获取最新的且没有抓取的video id 集合
	 * @param topN
	 * @return
	 */
	private List<String> popupUnfetchedList(int topN){
		List<String> list = new ArrayList<String>();
		synchronized(youtubeIds){
			for(int i=0;i<topN && index<youtubeIdLen;index++){
				String id = youtubeIds.get(index);
				UUID videoId = UUIDUtils.getMd5UUID(id);
				if(videodao.isVideoExists(videoId)){
					log.warn("video [{}] have exists in db.",id);
					continue;
				}
				list.add(id);
				i++;
			}
			
		}
		return list;
	}

	/**
	 * this method will be call with multiple-thread
	 * @throws  
	 * 
	 */
	@Override
	public List<FetchDatum> fetchDatumList() {
		int fromIndex = index;
		List<String> tops = popupUnfetchedList(1);
		String videos = StringUtils.join(tops, "|");
		String url = VideoApiUrl+"?q="+videos;
		List<FetchDatum> resultList = fetchVideoFeed(myService,url,feedName);
		if(resultList.size()>0){
			// set the fromIndex value to currentFetchState.
			// we only set the value to the last item, it will change the currentFetchState of the CrawlDB.
			FetchDatum fetchDatum = resultList.get(resultList.size()-1);
			fetchDatum.setCurrent(String.valueOf(fromIndex));
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
    				videoFeed = service.getFeed(new URL(feedUrl),VideoFeed.class);
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
	
	@Override
	public boolean isComplete(){
		return index>=youtubeIdLen;
	}
}
