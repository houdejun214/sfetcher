package com.sdata.component.fetcher;

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ArrayBlockingQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.lakeside.core.utils.StringUtils;
import com.mongodb.MongoException;
import com.sdata.component.data.dao.GroupMgDao;
import com.sdata.component.data.dao.LeafUserMgDao;
import com.sdata.component.data.dao.UserMgDao;
import com.sdata.component.parser.FlickrParser;
import com.sdata.component.parser.FlickrUserDetailParser;
import com.sdata.component.parser.FlickrUserRelationParser;
import com.sdata.component.site.FlickrApi;
import com.sdata.core.Configuration;
import com.sdata.core.Constants;
import com.sdata.core.FetchDatum;
import com.sdata.core.NegligibleException;
import com.sdata.core.RawContent;
import com.sdata.core.RunState;
import com.sdata.core.parser.ParseResult;
import com.sdata.core.parser.SdataParser;
import com.sdata.core.util.WebPageDownloader;

/**
 * fetch user relationship of Tencent weibo
 * 
 * the fetcher use multiple-threads to fetch list of a user' relationship 
 * 
 * @author houdj
 *
 */
public class FlickrGroupDetailFetcher extends FlickrBaseFetcher{
	
	private static final Logger log = LoggerFactory.getLogger("SdataCrawler.FlickrGroupDetailFetcher");
	private Queue<String> groupsQueryKey =null;
	
	private Boolean isComplete = false;
	
	private int topN = 100;
	
	private final FlickrApi flickrAPI;
	
	
	private SdataParser imagePaser;
	private SdataParser userParser;
	
	private UserMgDao userdao = new UserMgDao();
	private LeafUserMgDao leafuserdao = new LeafUserMgDao();
	private GroupMgDao groupdao = new GroupMgDao();
	
	private int lastCount;
	
	private int pageImage=0;
	private int pageMember=0;
	private String groupId;
	private String currentFetchState;

	public FlickrGroupDetailFetcher(Configuration conf,RunState state) throws UnknownHostException, MongoException {
		this.setConf(conf);
		this.setRunState(state);
		this.parser = new FlickrUserDetailParser(conf,state);
		this.imagePaser = new FlickrParser(conf,state);
		this.userParser = new FlickrUserRelationParser(conf,state);
		flickrAPI = new FlickrApi();
		String host = this.getConf("mongoHost");
		int port = this.getConfInt("mongoPort",27017);
		String dbName = this.getConf("mongoDbName");
		this.userdao.initilize(host, port, dbName);
		this.leafuserdao.initilize(host, port, dbName);
		this.groupdao.initilize(host, port, dbName);
		String currFetchState = state.getCurrentFetchState();
		this.initGroupsQueryKey(currFetchState,topN);
	}
	
	/**
	 * this method will be call with multiple-thread
	 * @throws  
	 * 
	 */
	@Override
	public List<FetchDatum> fetchDatumList() {
		if(groupsQueryKey==null|| groupsQueryKey.size()==0){
			List<String> fetchList = groupdao.getNextFetchList(lastCount,topN);
			if(fetchList.size()==0){
				this.isComplete = true;
			}else{
				groupsQueryKey = new ArrayBlockingQueue<String>(topN);
				groupsQueryKey.addAll(fetchList);
			}
		}
		List<FetchDatum> resultList = new ArrayList<FetchDatum>();
		if(StringUtils.isEmpty(groupId)){
			String id = groupsQueryKey.poll();
			lastCount++;
			currentFetchState = StringUtils.valueOf(lastCount);
			groupId = StringUtils.valueOf(id);
			pageImage = 1;
			pageMember = 1;
		}
//		String id = "77305426@N02";
		if(StringUtils.isNotEmpty(groupId)){
			//get group's imageslist by groupId then add to imagesList
			List<Map<String, Object>> groupImageslist =new ArrayList<Map<String, Object>>();
			if(pageImage!=-1){
				groupImageslist = this.getGroupImagesList(groupId,groupImageslist);
				for(int i=0;i<groupImageslist.size();i++){
					Map<String, Object> image = groupImageslist.get(i);
					String photoId = StringUtils.valueOf(image.get("id"));
//					if(imagedao.isImageExists(photoId)){// if this image had exists in mongoDB,remove this image
//						groupImageslist.remove(i);
//						i--;
//						continue;
//					}
					String owner = StringUtils.valueOf(image.get("owner"));
					String photourl = "http://www.flickr.com/photos/"+owner+"/"+photoId+"/";
					FetchDatum datum = new FetchDatum();
					datum.setCurrent(StringUtils.valueOf(currentFetchState));
					datum.setId(photoId);
					datum.setName(Constants.FLICKR_IMAGE);
					datum.setUrl(photourl);
					resultList.add(datum);
				}
			}
			if(pageMember!=-1){
				//get group's memberslist by groupId then add to contactList
				List<Map<String, Object>> membersList =new ArrayList<Map<String, Object>>();
				membersList = this.getGroupMembersList(groupId,membersList);
				for(int i=0;i<membersList.size();i++){
					Map<String, Object> contact = membersList.get(i);
					String userId = StringUtils.valueOf(contact.get("nsid"));
//					String id_s = userId.replace("@", "0").replace("N", "1");
//					Long _id = Long.valueOf(id_s);
//					if(userdao.isExists(_id)||leafuserdao.isExists(_id)){
//						membersList.remove(i);
//						i--;
//						continue;
//					}
					FetchDatum datum = new FetchDatum();
					datum.setCurrent(StringUtils.valueOf(currentFetchState));
					datum.setId(userId);
					datum.setName(Constants.FLICKR_USER);
					resultList.add(datum);
				}
			}
			if(pageImage == -1 && pageMember == -1){
				groupId = null;
			}
		}
		return resultList;
	}
	
	@Override
	public FetchDatum fetchDatum(FetchDatum datum) {
		String name = datum.getName();
		if(Constants.FLICKR_IMAGE.equals(name)){
			String photoId = datum.getId().toString();
			String photourl = datum.getUrl();
			String content = WebPageDownloader.download(photourl);
			//datum.setContent(content);
			RawContent rawContent = new RawContent(photourl,content);
			ParseResult result = imagePaser.parseSingle(rawContent);
			if(result != null){
				datum.setMetadata((Map<String, Object>)result.getMetadata());
			}else{
				log.info("fetch image["+photoId+"] has something wrong,then pass this image.ImageUrl is:"+photourl);
			}
		}else if(Constants.FLICKR_USER.equals(name)){
			String userId = datum.getId().toString();
			Map<String, Object> contact = this.fetchUserMeta(userId,userParser);
			datum.setMetadata(contact);
		}
		log.info("success fetch type["+name+"] id["+datum.getId()+"].");
		return datum;
	}
	
	private List<Map<String, Object>> getGroupMembersList(String id,  List<Map<String,Object>> membersList) {
		log.info("fetch group["+id+"]'s members list ");
		if(id.equals("")){
			throw new RuntimeException("group id is empty");
		}
		String content = null;
		try {
			String queryUrl = flickrAPI.getGroupMembersUrl(id,pageMember);
			log.info("fetching group["+id+"]'s members list ,url is: "+queryUrl);
			content = WebPageDownloader.download(queryUrl);
		} catch (Exception e) {
			e.printStackTrace();
		}
		RawContent rawContent = new RawContent(content);
		rawContent.setMetadata("type", Constants.PARSER_TYPE_MEMBERLIST);
		ParseResult result = parser.parseList(rawContent);
		if(result == null){
			throw new NegligibleException("fetch content is empty");
		}
		Map<?, ?> metadata = result.getMetadata();
		String stat = StringUtils.valueOf(metadata.get("stat"));
		String message = StringUtils.valueOf(metadata.get("message"));
		if(!stat.equals("ok")){
			pageMember = -1;
			log.warn("fetch Group["+id+"]'s members list stat ["+stat+"]:"+message);
			return membersList;
		}
		int totalPages =Integer.valueOf(StringUtils.valueOf(metadata.get("pages")));
		if(totalPages==0){
			pageMember = -1;
			log.warn("group["+id+"] do't has any members " );
			return membersList;
		}
		List<Map<String,Object>> queryImageList = (List<Map<String,Object>>)metadata.get(Constants.FLICKR_MEMBERLIST);
		membersList.addAll(queryImageList);
		int curPage = Integer.valueOf(StringUtils.valueOf(metadata.get("page")));
		if(curPage>=totalPages){//means this page is the last page
			pageMember = -1;
			return membersList;
		}
		log.info("fetch Group["+id+"]in page["+pageMember+"]'s members list count:"+membersList.size());
		pageMember++;
		return membersList;
	}
	
	private List<Map<String, Object>> getGroupImagesList(String id,  List<Map<String,Object>> imagesList) {
		log.info("fetch group["+id+"]'s images list ");
		if(id.equals("")){
			throw new RuntimeException("group id is empty");
		}
		String content = null;
		try {
			String queryUrl = flickrAPI.getGroupImagesUrl(id,pageImage);
			log.info("fetching group["+id+"]'s images list ,url is: "+queryUrl);
			content = WebPageDownloader.download(queryUrl);
		} catch (Exception e) {
			e.printStackTrace();
		}
		RawContent rawContent = new RawContent(content);
		rawContent.setMetadata("type", Constants.PARSER_TYPE_IMAGELIST);
		ParseResult result = parser.parseList(rawContent);
		if(result == null){
			throw new NegligibleException("fetch content is empty");
		}
		Map<?, ?> metadata = result.getMetadata();
		String stat = StringUtils.valueOf(metadata.get("stat"));
		String message = StringUtils.valueOf(metadata.get("message"));
		if(!stat.equals("ok")){
			pageImage=-1;
			log.warn("fetch Group["+id+"]'s images list stat ["+stat+"]:"+message);
			return imagesList;
		}
		int totalPages =Integer.valueOf(StringUtils.valueOf(metadata.get("pages")));
		if(totalPages==0){
			pageImage=-1;
			log.warn("group["+id+"] do't has any images " );
			return imagesList;
		}
		List<Map<String,Object>> queryImageList = (List<Map<String,Object>>)metadata.get(Constants.FLICKR_IMAGELIST);
		imagesList.addAll(queryImageList);
		int curPage = Integer.valueOf(StringUtils.valueOf(metadata.get("page")));
		if(curPage>=totalPages){//means this page is the last page
			pageImage=-1;
			return imagesList;
		}
		log.info("fetch Group["+id+"] in page["+pageImage+"]'s images list count:"+imagesList.size());
		pageImage++;
		return imagesList;
	}
	
	public void initGroupsQueryKey(String currFetchState,int topN){
		if(StringUtils.isEmpty(currFetchState) || "null".equals(currFetchState)){
			lastCount = 0;
		}else{
			lastCount = Integer.valueOf(currFetchState);
		}
		List<String>fetchList = groupdao.getNextFetchList(lastCount,topN);
		groupsQueryKey = new ArrayBlockingQueue<String>(topN);
		groupsQueryKey.addAll(fetchList);
	}
	
	
	/**
	 * move to next crawl instance
	 * @param curUser 
	 */
	protected void moveNext() {
	}
	
	
	@Override
	public void datumFinish(FetchDatum datum) {
		state.updateCurrentFetchState(datum.getCurrent());
	}

	@Override
	public boolean isComplete(){
		return isComplete;
	}
	
}
