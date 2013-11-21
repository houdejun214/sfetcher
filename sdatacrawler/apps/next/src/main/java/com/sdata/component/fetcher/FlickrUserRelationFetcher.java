package com.sdata.component.fetcher;

import java.io.File;
import java.io.IOException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ArrayBlockingQueue;

import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.lakeside.core.utils.StringUtils;
import com.mongodb.MongoException;
import com.sdata.component.data.dao.LeafUserMgDao;
import com.sdata.component.data.dao.UserMgDao;
import com.sdata.component.parser.FlickrUserRelationParser;
import com.sdata.component.site.FlickrApi;
import com.sdata.core.Configuration;
import com.sdata.core.Constants;
import com.sdata.core.CrawlAppContext;
import com.sdata.core.FetchDatum;
import com.sdata.core.NegligibleException;
import com.sdata.core.RawContent;
import com.sdata.core.RunState;
import com.sdata.core.crawldb.CrawlDBQueue;
import com.sdata.core.parser.ParseResult;
import com.sdata.core.util.ApplicationResourceUtils;
import com.sdata.core.util.WebPageDownloader;

/**
 * fetch user relationship of Tencent weibo
 * 
 * the fetcher use multiple-threads to fetch list of a user' relationship 
 * 
 * @author houdj
 *
 */
public class FlickrUserRelationFetcher extends FlickrBaseFetcher{
	
	private static final Logger log = LoggerFactory.getLogger("SdataCrawler.FlickrUserRelationFetcher");
	private Queue<String> usersQueryKey =null;
	private CrawlDBQueue crawlDB;
	
	private Boolean isComplete = false;
	
	private int topN = 100;
	
	private int MaxUsersNumPerKey = 100;
	
	private final FlickrApi flickrAPI;
	
	private final FlickrSearchAPI searchApi;
	
	private static final String KEY = "key";
	
	private Map<String,String> allUserMap;
	
	private UserMgDao userdao = new UserMgDao();
	private LeafUserMgDao leafuserdao = new LeafUserMgDao();
	
//	private int page;
	
	private Queue<Map<String,Object>> usersList;
	
	public FlickrUserRelationFetcher(Configuration conf,RunState state) throws UnknownHostException, MongoException {
		this.setConf(conf);
		this.setRunState(state);
		MaxUsersNumPerKey = this.getConfInt("MaxUsersNumPerKey", 100);
		crawlDB = CrawlAppContext.db;
		this.parser = new FlickrUserRelationParser(conf,state);
		flickrAPI = new FlickrApi();
		Date confStartTime = getConfDate(FlickrSearchAPI.START_TIME);
		Date confEndTime = getConfDate(FlickrSearchAPI.END_TIME);
		searchApi = new FlickrSearchAPI(confStartTime,confEndTime);
		allUserMap = new HashMap<String,String>();
		String host = this.getConf("mongoHost");
		int port = this.getConfInt("mongoPort",27017);
		String dbName = this.getConf("mongoDbName");
		this.userdao.initilize(host, port, dbName);
		this.leafuserdao.initilize(host, port, dbName);
		initUsersQueryKey(state);
	}

	/**
	 * this method will be call with multiple-thread
	 * 
	 */
	@Override
	public List<FetchDatum> fetchDatumList() {
		String curQueryKey = null;
		List<FetchDatum> resultList = new ArrayList<FetchDatum>();
		if(usersQueryKey!=null && usersQueryKey.size()>0){
			curQueryKey = usersQueryKey.poll();
			state.updateCurrentFetchState(curQueryKey+",0");
		}
		if(!StringUtils.isEmpty(curQueryKey)){
			List<Map<String, Object>> usersList = this.getKeySearchList(curQueryKey);
			crawlDB.insertQueueObjects(usersList);
			moveNext();
		}else{
			state.updateCurrentFetchState(Constants.KEYWORD_QUERY_OVER+",0");
			Map<String,Object> item = new HashMap<String,Object>();
			synchronized(this){
				if(usersList==null){
					usersList = new ArrayBlockingQueue<Map<String,Object>>(topN);
				}
				if(usersList.size()==0){
					usersList.addAll(crawlDB.queryQueue(topN));
					if(usersList.size()==0){
						this.isComplete=true;
						return null;
					}
				}
				item = usersList.poll();
			}
			if(item!=null){
				FetchDatum userDatum = new FetchDatum();
				userDatum.addAllMetadata(item);
				userDatum.setCurrent(Constants.KEYWORD_QUERY_OVER+",0");
				//get friend list of the seeds;
				Map<String, Object> curUser=userDatum.getMetadata();
				if(curUser!=null){
					String id = StringUtils.valueOf(curUser.get(KEY));
					List<Map<String, Object>> contactList =new ArrayList<Map<String, Object>>();
					if("0".equals(curUser.get("depth").toString())){
						Map<String, Object> seedUser = fetchUserMeta(id,parser);
						seedUser.put("isseed", "true");
						curUser.put(Constants.USER,seedUser);
					}
					this.getContactList(id, contactList);
					if(!contactList.isEmpty()){
						for(int i=0;i<contactList.size();i++){
							Map<String, Object> contact = contactList.get(i);
							String contactId = StringUtils.valueOf(contact.get("nsid"));
							String idStr = contactId.replace("@", "0").replace("N", "1");
							Long idL = Long.valueOf(idStr);
							if(!userdao.isExists(idL)&&!leafuserdao.isExists(idL)){
								FetchDatum datum = new FetchDatum();
								Map<String, Object> curFri = new HashMap<String, Object>();
								Map<String, Object> user = new HashMap<String, Object>();
								user.put("isseed", "false");
								curFri.put(Constants.USER,user);
								datum.setMetadata(curFri);
								datum.addMetadata(Constants.OBJECT_ID, Long.parseLong(idStr));
								datum.addMetadata(Constants.USER_ID, contactId);
								datum.setId(contactId);
								datum.setName(Constants.FLICKR_USER);
								datum.setCurrent(Constants.KEYWORD_QUERY_OVER+",0");
								//put friend to list
								resultList.add(datum);
							}
						}
					}
					String id_s = id.replace("@", "0").replace("N", "1");
					Long _id = Long.valueOf(id_s);
					curUser.put(Constants.OBJECT_ID, _id);
					userDatum.addMetadata(Constants.USER_ID, id);
					userDatum.setId(id);
					userDatum.setName(Constants.FLICKR_USER);
					userDatum.setMetadata(curUser);
					//put seed into List
					resultList.add(userDatum);
				}
			}
			moveNext();
		}
		return resultList;
	}
	
	@Override
	/**
	 *fetch friend's Contacts
	 */
	public FetchDatum fetchDatum(FetchDatum datum) {
		Map<String, Object> curUser=datum.getMetadata();
		if(curUser!=null){
			Map<String, Object> user = (Map<String, Object>)curUser.get(Constants.USER);
			String id = StringUtils.valueOf(curUser.get(Constants.USER_ID));
			if(id==null||id.equals("")){
				return datum;
			}
			String isSeed = StringUtils.valueOf(user.get("isseed"));
			// get user's detail
			user = fetchUserMeta(id,parser);
			user.put("isseed",(isSeed==null||isSeed.equals(""))?"false":isSeed);
 			List<Map<String, Object>> contactList =new ArrayList<Map<String, Object>>();
			this.getContactList(id, contactList);
			ArrayList<String> conList = new ArrayList<String>();
			for(Map<String,Object> con:contactList){
				conList.add(StringUtils.valueOf(con.get("nsid")));
			}
			user.put(Constants.FLICKR_CONTACTLIST, conList);
			String id_s = id.replace("@", "0").replace("N", "1");
			Long _id = Long.valueOf(id_s);
			curUser.put(Constants.USER,user);
			curUser.put(Constants.OBJECT_ID, _id);
			datum.setMetadata(curUser);
		}
		return datum;
	}
	

	/**
	 * get users from query br keyword
	 * @param key
	 * @return
	 * @author qiumm
	 */
	private List<Map<String, Object>> getKeySearchList(String key) {
		log.info("fetch KEY:["+key+"]'s users list ");
		List<Map<String,Object>> resultList = new ArrayList<Map<String,Object>>();
		while(StringUtils.isNotEmpty(key)){
			if(key.equals("")){
				throw new RuntimeException("query key is empty");
			}
			String content = searchApi.search(key);
			RawContent rawContent = new RawContent(content);
			ParseResult result = parser.parseList(rawContent);
			if(result == null){
				throw new NegligibleException("fetch content is empty");
			}
			Map<?, ?> metadata = result.getMetadata();
			if(null==metadata){
				log.warn("Get data is null!May be the source is change!");
				continue;
			}
			String stat = StringUtils.valueOf(metadata.get("stat"));
			if(!stat.equals("ok")){
				String message = StringUtils.valueOf(metadata.get("message"));
				log.warn("fetch KEY["+key+"]'s users list stat ["+stat+"]:"+message);
				return resultList;
			}
			List<Map<String,Object>> usersList = (List<Map<String,Object>>)metadata.get(Constants.USER);
			for(Map<String,Object> usermap:usersList){
				String mapKey =StringUtils.valueOf(usermap.get(KEY)); 
				if(!allUserMap.containsKey(mapKey)){
					allUserMap.put(mapKey, mapKey);
					resultList.add(usermap);
				}
			}
			if(resultList.size()>=MaxUsersNumPerKey){//means this key has complete
				break;
			}
			searchApi.setResultList(usersList);
			if(!searchApi.moveNext()){
				log.info("【users is not enough】fetch KEY:["+key+"]'s users list count:"+resultList.size());
				break;
			}
		}
		return resultList;
	}
	
	
	/**
	 * get user's contacts list
	 * @param datum
	 * @param page
	 * @param listenList
	 * @return
	 * @author qiumm
	 */
	@SuppressWarnings("unchecked")
	private List<Map<String, Object>> getContactList(String id,  List<Map<String,Object>> contactList) {
//		log.info("fetch user["+id+"]'s listen list ");
		int page = 1;
		while(StringUtils.isNotEmpty(id)){
			if(id.equals("")){
				throw new RuntimeException("user id is empty");
			}
			String content = null;
			try {
				String queryUrl = flickrAPI.getUserContactsUrl(id,page);
//				log.info("fetching user["+id+"] contact list ,url is: "+queryUrl);
				content = WebPageDownloader.download(queryUrl);
			} catch (Exception e) {
				e.printStackTrace();
			}
			RawContent rawContent = new RawContent(content);
			rawContent.setMetadata("type", Constants.PARSER_TYPE_USERLIST);
			ParseResult result = parser.parseSingle(rawContent);
			if(result == null){
				throw new NegligibleException("fetch content is empty");
			}
			Map<?, ?> metadata = result.getMetadata();
			String stat = StringUtils.valueOf(metadata.get("stat"));
			String message = StringUtils.valueOf(metadata.get("message"));
			if(!stat.equals("ok")){
				log.warn("fetch user["+id+"]'s contact list stat ["+stat+"]:"+message);
				return contactList;
			}
			String totalPages = StringUtils.valueOf(metadata.get("pages"));
			if(totalPages.equals("0")){
				log.warn("user["+id+"] do't has any contact " );
				return contactList;
			}
			List<Map<String,Object>> usersList = (List<Map<String,Object>>)metadata.get(Constants.FLICKR_CONTACT);
			contactList.addAll(usersList);
			String curPage = StringUtils.valueOf(metadata.get("page"));
			if(Integer.parseInt(curPage)>=Integer.parseInt(totalPages)){//means this page is the last page
				break;
			}
			page++;
		}
//		log.info("fetch user["+id+"]'s contact list count:"+contactList.size());
		return contactList;
	}
	
//	private List<Map<String, Object>> getContactListForPage(String id,  List<Map<String,Object>> contactList) {
//		if(id.equals("")){
//			throw new RuntimeException("user id is empty");
//		}
//		String content = null;
//		try {
//			String queryUrl = flickrAPI.getUserContactsUrl(id,page);
//			log.info("fetching seed user["+id+"] contact list in page ["+page+"],url is: "+queryUrl);
//			content = WebPageDownloader.download(queryUrl);
//		} catch (Exception e) {
//			e.printStackTrace();
//		}
//		RawContent rawContent = new RawContent(content);
//		rawContent.setMetadata("type", Constants.PARSER_TYPE_USERLIST);
//		ParseResult result = parser.parseSingle(rawContent);
//		if(result == null){
//			throw new NegligibleException("fetch content is empty");
//		}
//		Map<?, ?> metadata = result.getMetadata();
//		String stat = StringUtils.valueOf(metadata.get("stat"));
//		String message = StringUtils.valueOf(metadata.get("message"));
//		if(!stat.equals("ok")){
//			page = -1;
//			log.warn("fetch seed user["+id+"]'s contact list in page ["+page+"] stat ["+stat+"]:"+message);
//			return contactList;
//		}
//		String totalPages = StringUtils.valueOf(metadata.get("pages"));
//		if(totalPages.equals("0")){
//			page = -1;
//			log.warn("user["+id+"] do't has any contact " );
//			return contactList;
//		}
//		List<Map<String,Object>> usersList = (List<Map<String,Object>>)metadata.get(Constants.FLICKR_CONTACT);
//		contactList.addAll(usersList);
//		String curPage = StringUtils.valueOf(metadata.get("page"));
//		if(Integer.parseInt(curPage)>=Integer.parseInt(totalPages)){//means this page is the last page
//			page = -1;
//			return contactList;
//		}
//		page++;
//		log.info("fetch seed user["+id+"]'s contact list in page ["+page+"] count:"+contactList.size());
//		return contactList;
//	}
	
	/**
	 * move to next crawl instance
	 * @param curUser 
	 */
	protected void moveNext() {
		if(usersQueryKey==null || usersQueryKey.size()<=0){
			synchronized(this){
				List<Map<String, Object>> tops = crawlDB.queryQueue(MaxUsersNumPerKey);
				if(tops.size()==0){
					isComplete = true;
				}
			}
		}
	}
	
	
	@Override
	public void datumFinish(FetchDatum datum) {
		Map<String, Object> curUser=datum.getMetadata();
		Map<String, Object> user = (Map<String, Object>)curUser.get(Constants.USER);
		String isSeed = StringUtils.valueOf(user.get("isseed"));
		if("true".equals(isSeed)){
			String id = StringUtils.valueOf(curUser.get(KEY));
			crawlDB.updateQueueComplete(id);
		}
		state.updateCurrentFetchState(Constants.KEYWORD_QUERY_OVER+",0");
		datum.setCurrent(Constants.KEYWORD_QUERY_OVER+",0");
	}

	@Override
	public boolean isComplete(){
		return isComplete;
	}
	
	private void initUsersQueryKey(RunState state){
		String currentFetchState = state.getCurrentFetchState();
		String currentFetchKey = null;
		if(!StringUtils.isEmpty(currentFetchState)){
			String[] currentFetch = currentFetchState.split(",");
			currentFetchKey = currentFetch[0];
		}
		if(Constants.KEYWORD_QUERY_OVER.equals(currentFetchKey)){
			usersQueryKey = null;

			return;
		}
		List<String> list = new ArrayList<String>();
		String conf = this.getConf("userSeedFile");
		String path = ApplicationResourceUtils.getResourceUrl(conf);
		int length = 0 ;
		try {
			List<String> lines = FileUtils.readLines(new File(path));
			for(String line:lines){
				if(line.equals(currentFetchKey)){//last task stop when fetch this key
					list = new ArrayList<String>();
					list.add(line);
					length = 1;
				}else{
					list.add(line);
					length++;
				}
			}
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
		usersQueryKey = new ArrayBlockingQueue<String>(length);
		usersQueryKey.addAll(list);
	}
	

}
