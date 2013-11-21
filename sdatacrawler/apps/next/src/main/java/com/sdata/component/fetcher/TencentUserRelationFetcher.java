package com.sdata.component.fetcher;

import java.io.File;
import java.io.IOException;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;

import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.lakeside.core.utils.StringUtils;
import com.lakeside.core.utils.time.StopWatch;
import com.mongodb.MongoException;
import com.sdata.component.data.SdataUserRelationDbStorer;
import com.sdata.component.parser.TencentTweetsParser;
import com.sdata.component.parser.TencentUserRelationParser;
import com.sdata.core.Configuration;
import com.sdata.core.Constants;
import com.sdata.core.CrawlAppContext;
import com.sdata.core.FetchDatum;
import com.sdata.core.QueueStatus;
import com.sdata.core.RawContent;
import com.sdata.core.RunState;
import com.sdata.core.crawldb.CrawlDBQueue;
import com.sdata.core.fetcher.SdataFetcher;
import com.sdata.core.parser.ParseResult;
import com.sdata.core.parser.SdataParser;
import com.sdata.core.util.ApplicationResourceUtils;
import com.tencent.weibo.api.FriendsAPI;
import com.tencent.weibo.api.UserAPI;
import com.tencent.weibo.constants.OAuthConstants;
import com.tencent.weibo.oauthv1.OAuthV1;

/**
 * fetch user relationship of Tencent weibo
 * 
 * the fetcher use multiple-threads to fetch list of a user' relationship 
 * 
 * @author houdj
 *
 */
public class TencentUserRelationFetcher extends SdataFetcher{
	
	private static final Logger log = LoggerFactory.getLogger("SdataCrawler.TencentUserRelationFetcher");
	private CrawlDBQueue crawlDB;
	private int topN;
	private FriendsAPI friendAPI;
	private UserAPI userAPI;
	private OAuthV1 oauth;
	
	private static final String format = "json";
	private static final String reqnum = "30";//每次请求的users条数,最大30条
	private static final String STATUS = "status";
	private static final String DEPTH = "depth";
	private static final String KEY = "key";
	private static final String NAME = "name";
	
	private SdataParser userParse;
	private int maxUserNum = 0;
	
	private Queue<Map<String, Object>> users =null;
	private Boolean isComplete = false;
	private String lastDepth="";
	private boolean isEdge = false;
	
	public TencentUserRelationFetcher(Configuration conf,RunState state) throws UnknownHostException, MongoException {
		this.setConf(conf);
		this.setRunState(state);
		crawlDB = CrawlAppContext.db;
		this.parser = new TencentUserRelationParser(conf,state);
		String consumerKey = conf.get("ConsumerKey");
		String consumerSecret = conf.get("ConsumerSecret");
		String AccessToken = conf.get("AccessToken");
		String AccessTokenSecret = conf.get("AccessTokenSecret");
		oauth = new OAuthV1();
		oauth.setOauthConsumerKey(consumerKey);
		oauth.setOauthConsumerSecret(consumerSecret);
		oauth.setOauthToken(AccessToken);
		oauth.setOauthTokenSecret(AccessTokenSecret);
		friendAPI = new FriendsAPI(OAuthConstants.OAUTH_VERSION_1);
		userAPI = new UserAPI(OAuthConstants.OAUTH_VERSION_1);
		userParse = new TencentTweetsParser(this.getConf(),state);
		maxUserNum = this.getConfInt("MaxUserNum", 1000);
		topN = this.getConfInt("fetchQueueSize", 500);
	}
	
	@Override
	public void taskInitialize() {
		isComplete = false;
		isEdge = false;
		lastDepth = "";
		initUsers();
	}

	/**
	 * this method will be call with multiple-thread
	 * 
	 */
	@Override
	public List<FetchDatum> fetchDatumList() {
		Map<String, Object> curUser=null;
		if(users!=null && users.size()>0){
			curUser = users.poll();
		}
		if(curUser!=null){
			String curDepth = StringUtils.valueOf(curUser.get(DEPTH));
			if(StringUtils.isEmpty(lastDepth)){
				lastDepth = curDepth;
			}
			// change to other depth, cause the thread to wait until all items of last depth have been fetched.
			if(!curDepth.equals(lastDepth)){
				while(!curDepth.equals(lastDepth) && !crawlDB.isQueueDepthComplete(lastDepth)){
					log.debug("wait for last depth [{}] complete,current depth [{}]",lastDepth,curDepth);
					await(3000);
				}
			}
			int depth = StringUtils.toInt(curDepth);
			String name = StringUtils.valueOf(curUser.get(NAME));
			int queueTotoalCount = maxUserNum;
			if(!isEdge) {
				queueTotoalCount = crawlDB.queryQueueTotoalCount();
			}
			List<FetchDatum> list = new ArrayList<FetchDatum>();
			log.info("["+curDepth+"] fetch user["+name+"]'s friends list ");
			// get friend list of the user;
			List<Map<String, Object>> relatedList =new ArrayList<Map<String, Object>>();
			this.getListenList(name, relatedList);
			String childDepth = String.valueOf(depth+1);
			for(Map<String,Object> item:relatedList){
				item.put(KEY, item.get("name"));
				item.put(DEPTH, childDepth);
			}
			FetchDatum userDatum = new FetchDatum();
			// isEdge indicate the queue size have reach the maximum size of the require
			if(!isEdge && queueTotoalCount<maxUserNum){
				if((relatedList.size()+queueTotoalCount)>maxUserNum){
					relatedList = relatedList.subList(0, maxUserNum - queueTotoalCount);
				}
				this.appendQueueUsers(relatedList);
			}else{
				isEdge = true;
				userDatum.addMetadata("isEdge", true);
			}
			userDatum.addMetadata("listens", relatedList);
			userDatum.addMetadata("name", name);
			userDatum.addMetadata(DEPTH, depth);
			list.add(userDatum);
			// set current user process is complete, this operation best be after appending operation
			moveNext(curUser);
			lastDepth = curDepth;
			return list;
		}
		return null;
	}
	

	@Override
	public FetchDatum fetchDatum(FetchDatum datum) {
		Map<String, Object> userRelation = datum.getMetadata();
		String name = StringUtils.valueOf(userRelation.get(NAME));
		// set current user deal is complete, this operation best be after appending operation
		if("0".equals(userRelation.get("depth"))){
			userRelation.put(Constants.USER,fetchUserMeta(name));
		}
		UUID uid = StringUtils.getMd5UUID(name);
		//userRelation.put(Constants.OBJECT_ID, uid);
		userRelation.put("uid", uid);
		return datum;
	}
	
	@Override
	public void datumFinish(FetchDatum datum) {
		String name = datum.getMeta("name");
		crawlDB.updateQueueComplete(name);
	}

	@SuppressWarnings("unchecked")
	private Map<String,Object> fetchUserMeta(String name){
		String userJson = null;
		int retryCount=1;
		while(retryCount<=20){
			try {
				userJson = userAPI.otherInfo(oauth, "json", name,null);
				if(StringUtils.isEmpty(userJson)){
					continue;
				}else{
					break;
				}
			} catch (Exception e) {
				if(e instanceof IOException || e instanceof SocketTimeoutException || e.getCause() instanceof IOException){
					log.warn("socket exception [{}] when fetch [{}]'s friends, try again",e.getMessage(),name);
					this.await(5000);
				}else{
					e.printStackTrace();
					break;
				}
			}
			retryCount++;
		}
		//此处使用TencentTweetsParser类的parseSingle方法，专门parser用户信息
		ParseResult parseResult = userParse.parseSingle(new RawContent(userJson));
		Map<?, ?> userMap = parseResult.getMetadata();
		Map<?, ?> userInfoMap = (Map<?, ?>)userMap.get("user");
		String errcode =StringUtils.valueOf( userInfoMap.get("rerrcode"));
		if(!errcode.equals("0")){
			String msg = StringUtils.valueOf( userInfoMap.get("rmsg"));
			log.error("fetch tencent tweets 【"+name+"】 has error:【"+ msg +"】+"+"【"+errcode+"】");
		}
		Map<String, Object> map = ((Map<String, Object>) userMap.get(Constants.TENCENT_USER));
		return map;
	}

	/**
	 * 获取用户的关注列表
	 * @param datum
	 * @param page
	 * @param listenList
	 * @return
	 * @author qiumm
	 */
	@SuppressWarnings("unchecked")
	private List<Map<String, Object>> getListenList(String name,  List<Map<String,Object>> listenList) {
		int page = 0;
		while(StringUtils.isNotEmpty(name)){
			if(name.equals("")){
				throw new RuntimeException("user name is empty");
			}
			String content = null;
			int retryCount=1;
			while(retryCount<=20){
				try {
					content = friendAPI.userIdollist(oauth, format, reqnum,StringUtils.valueOf(page*30), name,null,"0");
					if(StringUtils.isEmpty(content)){
						continue;
					}else{
						break;
					}
				} catch (Exception e) {
					if(e instanceof IOException || e instanceof SocketTimeoutException || e.getCause() instanceof IOException){
						log.warn("socket exception [{}] when fetch [{}]'s friends, try again",e.getMessage(),name);
						this.await(5000);
					}else{
						e.printStackTrace();
						break;
					}
				}
				retryCount++;
			}
			RawContent rawContent = new RawContent(content);
			ParseResult result = parser.parseSingle(rawContent);
			if(result == null){
				log.warn("fetch [{}]'s friends content is empty",name);
				return listenList;
			}
			Map<?, ?> metadata = result.getMetadata();
			String errcode = StringUtils.valueOf(metadata.get("errcode"));
			String msg = StringUtils.valueOf(metadata.get("msg"));
			String ret = StringUtils.valueOf(metadata.get("ret"));
			if("2".equals(ret)){
				log.warn("fetch user["+name+"]'s listen list, wait for 300s as rate limit ");
				this.await(300000);
				continue;
			}else if("3".equals(ret)){
				log.warn("fetch user["+name+"]'s listen list, wait for 5s check sign error ");
				this.await(5000);
				continue;
			}else if(!errcode.equals("0")){
				log.warn("fetch user["+name+"]'s listen list， ret["+ret+"] error ["+errcode+"]:"+msg);
				return listenList;
			}
			
			List<Map<String,Object>> usersList = (List<Map<String,Object>>)metadata.get(Constants.USER);
			listenList.addAll(usersList);
			String hasnext = StringUtils.valueOf(metadata.get("hasnext"));
			if(hasnext.equals("1")){ //表示下页没有数据
				break;
			}
			page++;
		}
		//log.info("fetch user["+name+"]'s listen list count:"+listenList.size());
		return listenList;
	}
	

	/**
	 * move to next crawl instance
	 * @param curUser 
	 */
	protected void moveNext(Map<String, Object> curUser) {
		if(users.size()<=0){
			synchronized(this){
				if(users.size()<=0){
					List<Map<String, Object>> tops = crawlDB.queryQueue(topN);
					users.addAll(tops);
					if(users.size()==0){
						isComplete = true;
					}
				}
			}
		}
	}
	
	@Override
	public boolean isComplete(){
		return isComplete;
	}
	
	private void initUsers(){
		List<Map<String, Object>> list = crawlDB.queryQueue(topN);
		int size = crawlDB.queryQueueTotoalCount();
		if((list==null || list.size()==0) && size==0){
			// init the queue with user seeds
			if(list == null){
				list = new ArrayList<Map<String, Object>>();
			}
			String conf = this.getConf("userSeedFile");
			String path = ApplicationResourceUtils.getResourceUrl(conf);
			try {
				List<String> lines = FileUtils.readLines(new File(path));
				for(String line:lines){
					String[] splits = line.split(",");
					if(splits==null || splits.length!=2){
						throw new RuntimeException("there is invalid line in user seeds list!");
					}
					Map<String, Object> meta = new HashMap<String,Object>();
					meta.put(NAME, splits[0]);
					meta.put(DEPTH, "0");
					meta.put(STATUS, QueueStatus.RUNING);
					list.add(meta);
				}
			} catch (IOException e) {
				throw new RuntimeException(e);
			}
			appendQueueUsers(list);
		}else if(size>0 && list.size()==0){
			isComplete = true;
		}
		users = new ArrayBlockingQueue<Map<String,Object>>(topN*2);
		users.addAll(list);
	}
	
	private List<Map<String, Object>> appendQueueUsers(List<Map<String, Object>> list) {
		StopWatch watch = new StopWatch();
		watch.start();
		for(Map<String, Object> item:list){
			item.put(Constants.QUEUE_KEY, item.get("name"));
		}
		this.crawlDB.insertQueueObjects(list);
		watch.stop();
		long elapsedTime = watch.getElapsedTime();
		int size = 0;
		if(list!=null){
			size = list.size();
		}
		log.info("insert Queue [{}] elapse {}",size,elapsedTime);
		return list;
	}

	
	@Override
	public void taskFinish() {
		
		//
		SdataUserRelationDbStorer relationStorer = (SdataUserRelationDbStorer)this.storer;
		
		relationStorer.backup();
		//
		crawlDB.resetQueueStatus();
	}
}
