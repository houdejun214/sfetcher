package com.sdata.component.fetcher;

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ArrayBlockingQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.lakeside.core.utils.StringUtils;
import com.lakeside.core.utils.time.DateTimeUtils;
import com.mongodb.MongoException;
import com.nus.next.config.NSouceDB;
import com.nus.next.config.SourceConfig;
import com.sdata.component.data.dao.GroupMgDao;
import com.sdata.component.data.dao.ImageMgDao;
import com.sdata.component.data.dao.LeafUserMgDao;
import com.sdata.component.data.dao.UserMgDao;
import com.sdata.component.parser.FlickrParser;
import com.sdata.component.parser.FlickrUserDetailParser;
import com.sdata.component.parser.FlickrUserRelationParser;
import com.sdata.component.site.FlickrApi;
import com.sdata.core.Configuration;
import com.sdata.core.Constants;
import com.sdata.core.CrawlAppContext;
import com.sdata.core.FetchDatum;
import com.sdata.core.NegligibleException;
import com.sdata.core.RawContent;
import com.sdata.core.RunState;
import com.sdata.core.crawldb.CrawlDBRunState;
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
public class FlickrUserDetailFetcher extends FlickrBaseFetcher{
	
	private static final Logger log = LoggerFactory.getLogger("SdataCrawler.FlickrUserDetailFetcher");
	private Queue<String> usersQueryKey =null;
	private CrawlDBRunState crawlDB;
	
	private Boolean isComplete = false;
	
	private int topN = 100;
	
	private final FlickrApi flickrAPI;
	
	
	private SdataParser imagePaser;
	private SdataParser userParser;
	
	private UserMgDao userdao = new UserMgDao();
	private LeafUserMgDao leafuserdao = new LeafUserMgDao();
	private ImageMgDao imagedao =null;
	private GroupMgDao groupdao = new GroupMgDao();
	
	private int currentFetchCount;
	private int currentFetchImagePage;
	private int currentFetchGroupPage;
	private int currentFetchContactPage;
	private String userId;
	private String currentFetchState;
	
	private boolean isFirstUser;

	public FlickrUserDetailFetcher(Configuration conf,RunState state) throws UnknownHostException, MongoException {
		this.setConf(conf);
		this.setRunState(state);
		crawlDB = CrawlAppContext.db;
		this.parser = new FlickrUserDetailParser(conf,state);
		this.imagePaser = new FlickrParser(conf,state);
		this.userParser = new FlickrUserRelationParser(conf,state);
		flickrAPI = new FlickrApi();
		SourceConfig sourceConfig = SourceConfig.getInstance();
		NSouceDB sourceDB = sourceConfig.getSourceDB(conf.get(Constants.SOURCE));
		String host = sourceDB.getHost();
		int port =sourceDB.getPort();
		String dbName = sourceDB.getDbname();
		this.userdao.initilize(host, 27017, dbName);
		this.leafuserdao.initilize(host, port, dbName);
		this.groupdao.initilize(host, port, dbName);
		imagedao = new ImageMgDao(conf.get(Constants.SOURCE));
		currentFetchState = state.getCurrentFetchState();
		this.initUsersQueryKey(currentFetchState,topN);
		isFirstUser = true;
	}
	
	/**
	 * this method will be call with multiple-thread
	 * @throws  
	 * 
	 */
	@Override
	public List<FetchDatum> fetchDatumList() {
		if(usersQueryKey==null || usersQueryKey.size()==0){
			List<String> fetchList = userdao.getNextFetchList(currentFetchCount-1,topN);
			if(fetchList.size()==0){
//				this.isComplete = true;
				try {
					Thread.sleep(600000);
					return null;
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}else{
				usersQueryKey = new ArrayBlockingQueue<String>(topN);
				usersQueryKey.addAll(fetchList);
			}
		}
		if(StringUtils.isEmpty(userId)){
			userId = usersQueryKey.poll();
			currentFetchState = StringUtils.valueOf(currentFetchCount);
			currentFetchImagePage = 1;
			currentFetchGroupPage = 1;
			currentFetchContactPage = 1;
		}
		List<FetchDatum> resultList = new ArrayList<FetchDatum>();
		if(StringUtils.isNotEmpty(userId)){
			// get friend list of the user;
//			if(currentFetchContactPage!=-1){
//				List<Map<String, Object>> contactList =new ArrayList<Map<String, Object>>();
//				this.getContactList(userId, contactList);
//				for(int i=0;i<contactList.size();i++){
//					Map<String, Object> contact = contactList.get(i);
//					String contactId = StringUtils.valueOf(contact.get("nsid"));
//					String idStr = contactId.replace("@", "0").replace("N", "1");
//					contact.put(Constants.OBJECT_ID, Long.parseLong(idStr));
//				}
//			for(int i=0;i<contactList.size();i++){
//				Map<String, Object> contact = contactList.get(i);
//				String userId = StringUtils.valueOf(contact.get("nsid"));
////				String id_s = userId.replace("@", "0").replace("N", "1");
////				Long _id = Long.valueOf(id_s);
////				if(userdao.isExists(_id)||leafuserdao.isExists(_id)){
////					contactList.remove(i);
////					i--;
////					continue;
////				}
//				FetchDatum datum = new FetchDatum();
//				datum.setCurrent(StringUtils.valueOf(currentFetchState));
//				datum.setId(userId);
//				datum.setName(Constants.FLICKR_USER);
//				resultList.add(datum);
//			}
//			}
			currentFetchContactPage = -1;
			// get images list of the user;
			int repeatImagesCount = 0;
			if(currentFetchImagePage!=-1){
				List<Map<String, Object>> imagesList =new ArrayList<Map<String, Object>>();
				this.getImagesList(userId, imagesList);
				for(int i=0;i<imagesList.size();i++){
					Map<String, Object> image = imagesList.get(i);
					String photoId = StringUtils.valueOf(image.get("id"));
					if(imagedao.isImageExists(photoId)){// if this image had exists in mongoDB,remove this image
						imagesList.remove(i);
						i--;
						repeatImagesCount++;
						continue;
					}
					String owner = StringUtils.valueOf(image.get("owner"));
					String photourl = "http://www.flickr.com/photos/"+owner+"/"+photoId+"/";
					FetchDatum datum = new FetchDatum();
					datum.setCurrent(StringUtils.valueOf(currentFetchState));
					datum.setId(photoId);
					datum.setName(Constants.FLICKR_IMAGE);
					datum.setUrl(photourl);
					datum.addAllMetadata(image);
					resultList.add(datum);
				}
			}
			//由于更换了集群，导致原先的排序不能继续使用，故需从头开始爬取，此处定义：若账号下的图片已经存在了20张，则表明此人已经爬取完成。
			//此处对第一个账号不限制，因为程序可能异常中止，但是第一个账号已经下载了一部分图片，为保证此账号数据完成。
			if(!isFirstUser && repeatImagesCount>20){
				currentFetchImagePage=-1;
			}
			//每一个账号最多下载2000张图片
			if(currentFetchImagePage>=5){
				currentFetchImagePage=-1;
			}
			
			//get group list of the user
//			if(currentFetchGroupPage!=-1){
//				List<Map<String, Object>> groupsList =new ArrayList<Map<String, Object>>();
//				this.getGroupsList(userId, groupsList);
//				for(int i=0;i<groupsList.size();i++){
//					Map<String, Object> group = groupsList.get(i);
//					String groupId = StringUtils.valueOf(group.get("nsid"));
//					String id_s = groupId.replace("@", "0").replace("N", "1");
//					Long _id = Long.valueOf(id_s);
//					if(groupdao.isExists(_id)){
//						groupsList.remove(i);
//						i--;
//						continue;
//					}
//					FetchDatum datum = new FetchDatum();
//					datum.setCurrent(StringUtils.valueOf(currentFetchState));
//					datum.setId(groupId);
//					datum.setName(Constants.FLICKR_GROUP);
//					resultList.add(datum);
//				}
//			}
			currentFetchGroupPage = -1;
			//add to datum for return
			if(currentFetchImagePage == -1 && currentFetchContactPage == -1 && currentFetchGroupPage == -1){
				userId = null;
				currentFetchCount++;
				isFirstUser = false;
			}
		}
		return resultList;
	}
	
	@Override
	public FetchDatum fetchDatum(FetchDatum datum) {
		String name = datum.getName();
		if(Constants.FLICKR_IMAGE.equals(name)){
			Map<String,Object> imageInfo = datum.getMetadata();
			String photoId = datum.getId().toString();
			String photourl = datum.getUrl();
			String content = WebPageDownloader.download(photourl);
			//datum.setContent(content);
			RawContent rawContent = new RawContent(photourl,content);
			ParseResult result = imagePaser.parseSingle(rawContent);
			if(result != null){
				Map<String, Object> image = new HashMap<String,Object>();
				String takedateStr = StringUtils.valueOf(imageInfo.get("datetaken"));
				image.put("takenDate", DateTimeUtils.parse(takedateStr, "yyyy-MM-dd HH:mm:ss") );
				Long uploaddate = Long.valueOf(StringUtils.valueOf(imageInfo.get("dateupload")));
				image.put("uploadDate",DateTimeUtils.getTimeFromUnixTime(uploaddate) );
				image.put("isPublic", imageInfo.get("ispublic"));
				image.put("width_l", imageInfo.get("width_l"));
				image.put("width_o", imageInfo.get("width_o"));
				image.put("height_l", imageInfo.get("height_l"));
				image.put("height_o", imageInfo.get("height_o"));
				datum.clearMetadata();
				datum.addAllMetadata(image);
				datum.setMetadata((Map<String, Object>)result.getMetadata());
			}else{
				log.info("fetch image["+photoId+"] has something wrong,then pass this image.ImageUrl is:"+photourl);
			}
		}else if(Constants.FLICKR_USER.equals(name)){
			datum = null;
			return datum;
//			String userId = datum.getId();
//			Map<String, Object> contact = this.fetchUserMeta(userId,userParser);
//			datum.setMetadata(contact);
		}else if(Constants.FLICKR_GROUP.equals(name)){
//			String groupId = datum.getId();
//			Map<String, Object> group = this.fetchGroupInfo(groupId);
//			datum.setMetadata(group);
			datum = null;
			return datum;
		}
		log.info("success fetch type["+name+"] id["+datum.getId()+"].");
		return datum;
	}
	
	
	/**
	 * fetch user's  infomation
	 * @param id
	 * @return
	 * @author qiumm
	 */
	private Map<String,Object> fetchGroupInfo(String id){
		String groupInfoJson = null;
		try {
			String queryUrl = flickrAPI.getGroupInfoUrl(id);
			log.info("fetching group["+id+"] info ,url is: "+queryUrl);
			groupInfoJson = WebPageDownloader.download(queryUrl);
		} catch (Exception e) {
			e.printStackTrace();
		}
		RawContent rc = new RawContent(groupInfoJson);
		ParseResult parseResult = parser.parseSingle(rc);
		Map<?, ?> groupMap = parseResult.getMetadata();
		Map<String, Object> groupInfoMap = (Map<String, Object>)groupMap.get(Constants.FLICKR_GROUP);
		String stat = StringUtils.valueOf(groupInfoMap.get("stat"));
		String message = StringUtils.valueOf(groupInfoMap.get("message"));
		if(!stat.equals("ok")){
			log.warn("fetch group["+id+"]'s info stat ["+stat+"]:"+message);
		}
		
		String id_s = id.replace("@", "0").replace("N", "1");
		Long _id = Long.valueOf(id_s);
		groupInfoMap.put(Constants.OBJECT_ID, _id);
		
		Map<String, Object> map = groupInfoMap;
		return map;
		
	}
	

	/**
	 * get user's iamges list
	 * @param datum
	 * @param page
	 * @param listenList
	 * @return
	 * @author qiumm
	 */
	@SuppressWarnings("unchecked")
	private List<Map<String, Object>> getContactList(String id,  List<Map<String,Object>> contactList) {
		log.info("fetch user["+id+"]'s listen list ");
		if(id.equals("")){
			throw new RuntimeException("user id is empty");
		}
		String content = null;
		try {
			String queryUrl = flickrAPI.getUserContactsUrl(id,currentFetchContactPage);
			log.info("fetching user["+id+"] contact list ,url is: "+queryUrl);
			content = WebPageDownloader.download(queryUrl);
		} catch (Exception e) {
			e.printStackTrace();
		}
		RawContent rawContent = new RawContent(content);
		rawContent.setMetadata("type", Constants.PARSER_TYPE_USERLIST);
		ParseResult result = parser.parseList(rawContent);
		if(result == null){
			throw new NegligibleException("fetch content is empty");
		}
		Map<?, ?> metadata = result.getMetadata();
		String stat = StringUtils.valueOf(metadata.get("stat"));
		String message = StringUtils.valueOf(metadata.get("message"));
		if(!stat.equals("ok")){
			currentFetchContactPage = -1;
			log.warn("fetch user["+id+"]'s contact list stat ["+stat+"]:"+message);
			return contactList;
		}
		String totalPages = StringUtils.valueOf(metadata.get("pages"));
		if(totalPages.equals("0")){
			currentFetchContactPage = -1;
			log.warn("user["+id+"] do't has any contact " );
			return contactList;
		}
		List<Map<String,Object>> usersList = (List<Map<String,Object>>)metadata.get(Constants.FLICKR_CONTACT);
		contactList.addAll(usersList);
		String curPage = StringUtils.valueOf(metadata.get("page"));
		if(totalPages.equals(curPage)){//means this page is the last page
			currentFetchContactPage = -1;
			return contactList;
		}
		currentFetchContactPage++;
		log.info("fetch user["+id+"]'s contact In page[+currentFetchContactPage+] list count:"+contactList.size());
		return contactList;
	}
	
	private List<Map<String, Object>> getImagesList(String id,  List<Map<String,Object>> imagesList) {
		log.info("fetch user["+id+"]'s iamges list ");
		if(id.equals("")){
			throw new RuntimeException("user id is empty");
		}
		String content = null;
		try {
			String queryUrl = flickrAPI.getUserPhotoesUrl(id,currentFetchImagePage);
			log.info("fetching user["+id+"] Images list ,url is: "+queryUrl);
			content = WebPageDownloader.download(queryUrl);
		} catch (Exception e) {
			e.printStackTrace();
		}
		RawContent rawContent = new RawContent(content);
		rawContent.setMetadata("type", Constants.PARSER_TYPE_IMAGELIST);
		ParseResult result = parser.parseList(rawContent);
		if(result == null){
			currentFetchImagePage++;
			throw new NegligibleException("fetch content is empty");
		}
		Map<?, ?> metadata = result.getMetadata();
		String stat = StringUtils.valueOf(metadata.get("stat"));
		String message = StringUtils.valueOf(metadata.get("message"));
		if(!stat.equals("ok")){
			currentFetchImagePage = -1;
			log.warn("fetch user["+id+"]'s image list stat ["+stat+"]:"+message);
			return imagesList;
		}
		int totalPages =Integer.valueOf(StringUtils.valueOf(metadata.get("pages")));
		if(totalPages==0){
			currentFetchImagePage = -1;
			log.warn("user["+id+"] do't has any contact " );
			return imagesList;
		}
		List<Map<String,Object>> queryImageList = (List<Map<String,Object>>)metadata.get(Constants.FLICKR_IMAGELIST);
		imagesList.addAll(queryImageList);
		int curPage = Integer.valueOf(StringUtils.valueOf(metadata.get("page")));
		if(curPage>=totalPages){//means this page is the last page
			currentFetchImagePage = -1;
			return imagesList;
		}
		log.info("fetch user["+id+"]'s Image In page["+currentFetchImagePage+"] list count:"+imagesList.size());
		currentFetchImagePage++;
		//crawl 10000 photos at most everyone
		if(currentFetchImagePage>500){
			currentFetchImagePage = -1;
		}
		return imagesList;
	}
	
	
	private List<Map<String, Object>> getGroupsList(String id,  List<Map<String,Object>> groupsList) {
		log.info("fetch user["+id+"]'s Group list ");
		if(id.equals("")){
			throw new RuntimeException("user id is empty");
		}
		String content = null;
		try {
			String queryUrl = flickrAPI.getUserGroupsUrl(id);
			log.info("fetching user["+id+"] Group list ,url is: "+queryUrl);
			content = WebPageDownloader.download(queryUrl);
		} catch (Exception e) {
			e.printStackTrace();
		}
		RawContent rawContent = new RawContent(content);
		rawContent.setMetadata("type", Constants.PARSER_TYPE_GROUPLIST);
		ParseResult result = parser.parseList(rawContent);
		if(result == null){
			throw new NegligibleException("fetch content is empty");
		}
		Map<?, ?> metadata = result.getMetadata();
		String stat = StringUtils.valueOf(metadata.get("stat"));
		String message = StringUtils.valueOf(metadata.get("message"));
		if(!stat.equals("ok")){
			currentFetchGroupPage=-1;
			log.warn("fetch user["+id+"]'s Group list stat ["+stat+"]:"+message);
			return groupsList;
		}
		List<Map<String,Object>> queryImageList = (List<Map<String,Object>>)metadata.get(Constants.FLICKR_GROUPLIST);
		groupsList.addAll(queryImageList);
		log.info("fetch user["+id+"]'s Group list count:"+groupsList.size());
		currentFetchGroupPage=-1;
		return groupsList;
	}
	
	
	public void initUsersQueryKey(String currFetchState,int topN){
		
		if(StringUtils.isEmpty(currFetchState) || "null".equals(currFetchState)){
			currentFetchCount = 1;
		}else{
			currentFetchCount = Integer.valueOf(currFetchState);
		}
		List<String>fetchList = userdao.getNextFetchList(currentFetchCount-1,topN);
		usersQueryKey = new ArrayBlockingQueue<String>(topN);
		usersQueryKey.addAll(fetchList);
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
