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

import javax.mail.MessagingException;

import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.lakeside.core.utils.StringUtils;
import com.lakeside.core.utils.UUIDUtils;
import com.mongodb.DBObject;
import com.mongodb.MongoException;
import com.sdata.component.data.dao.TweetsMgDao;
import com.sdata.component.data.dao.UserMgDao;
import com.sdata.component.parser.TencentUserRelationParser;
import com.sdata.component.parser.TencentUserTweetsParser;
import com.sdata.core.Configuration;
import com.sdata.core.Constants;
import com.sdata.core.CrawlAppContext;
import com.sdata.core.FetchDatum;
import com.sdata.core.RawContent;
import com.sdata.core.RunState;
import com.sdata.core.crawldb.CrawlDBQueue;
import com.sdata.core.fetcher.SdataFetcher;
import com.sdata.core.parser.ParseResult;
import com.sdata.core.parser.SdataParser;
import com.sdata.core.util.ApplicationContextHolder;
import com.sdata.core.util.ApplicationResourceUtils;
import com.sdata.core.util.EmailUtil;
import com.tencent.weibo.api.StatusesAPI;
import com.tencent.weibo.api.TAPI;
import com.tencent.weibo.constants.OAuthConstants;
import com.tencent.weibo.oauthv1.OAuthV1;

/**
 * fetch user tweets of Tencent weibo
 * 
 * the fetcher use multiple-threads to fetch list of a user' tweets 
 * 
 * @author qmm
 *
 */
public class TencentUserTweetsFetcher extends SdataFetcher{
	
	private static final Logger log = LoggerFactory.getLogger("SdataCrawler.TencentUserTweetsFetcher");
	private Queue<Map<String, Object>> users =null;
	private CrawlDBQueue crawlDB;
	private Boolean isComplete = false;
	private int topN = 100;
	private StatusesAPI statusAPI;
	private OAuthV1 oauth;
	
	private static final String format = "json";
	private static final String reqnum = "30";//每次请求的users条数,最大30条
	private static final String DEPTH = "depth";
	private static final String NAME = "name";
	private static final String NICK = "nick";
	
	private SdataParser tweetsParse;
	private String lastId="";
	private Long maxCrawlLength;
	
	private String crawlerQueueTableName;
	
	
	public TencentUserTweetsFetcher(Configuration conf,RunState state) throws UnknownHostException, MongoException {
		this.setConf(conf);
		this.setRunState(state);
		crawlDB = CrawlAppContext.db;
		this.parser = new TencentUserRelationParser(conf,state);
		String consumerKey = conf.get("ConsumerKey");
		String consumerSecret = conf.get("ConsumerSecret");
		String AccessToken = conf.get("AccessToken");
		String AccessTokenSecret = conf.get("AccessTokenSecret");
		crawlerQueueTableName = conf.get("crawlerQueueTableName");
		maxCrawlLength =this.getConfLong("maxCrawlLength",20000000L);
		oauth = new OAuthV1();
		oauth.setOauthConsumerKey(consumerKey);
		oauth.setOauthConsumerSecret(consumerSecret);
		oauth.setOauthToken(AccessToken);
		oauth.setOauthTokenSecret(AccessTokenSecret);
		statusAPI = new StatusesAPI(OAuthConstants.OAUTH_VERSION_1);
		tweetsParse = new TencentUserTweetsParser(this.getConf(),state);
		if(StringUtils.isEmpty(state.getCurrentFetchState())){
			lastId = "0";
		}else{
			lastId = state.getCurrentFetchState();
		}
		initUsers();
	}

	/**
	 * this method will be call with multiple-thread
	 * 
	 */
	@Override
	public List<FetchDatum> fetchDatumList() {
		Long nowCrawlNum = Long.valueOf(lastId);
		if(nowCrawlNum.compareTo(maxCrawlLength)==1){
			this.isComplete = true;
			return null;
		}
		Map<String, Object> curUser=null;
		if(users!=null && users.size()>0){
			curUser = users.poll();
		}
		List<FetchDatum> list = new ArrayList<FetchDatum>();
		if(curUser!=null){
			String curId = StringUtils.valueOf(curUser.get("id"));
			String name = StringUtils.valueOf(curUser.get(NAME));
			Date lastTweetDtFromDb = this.getLastTweetDt(name);
			// get friend list of the user;
			List<FetchDatum> tweetsList =new ArrayList<FetchDatum>();
			//List<Map<String, Object>> followList =new ArrayList<Map<String, Object>>();
			this.await(1000);
			this.getTweetsList(name, tweetsList, lastTweetDtFromDb);
			if(tweetsList.size()>0){
				//update lastid
				FetchDatum lastDatum = tweetsList.get(tweetsList.size()-1);
				lastDatum.addMetadata("lastUserId", lastId);
				lastDatum.setCurrent(lastId);
				//update lastest date
				FetchDatum firstDatum = tweetsList.get(0);
				Date lastTweetDt2Db = (Date)(firstDatum.getMetadata().get("crtdt"));
				lastDatum.addMetadata(Constants.FAMOUS_NEWEST_TWEET_DATE, lastTweetDt2Db);
				tweetsList.set(tweetsList.size()-1, lastDatum);
			}
			log.info("【"+curUser.get(NAME)+"】 fetch tweets count:"+tweetsList.size());
			list.addAll(tweetsList);
			// set current user deal is complete, this operation best be after appending operation
			moveNext(curUser);
			lastId = curId;
			return list;
		}
		return list;
	}
	
	
	private Date getLastTweetDt(String name){
//		DBObject user =userdao.query(UUIDUtils.getMd5UUID(name));
//		if(null!=user){
//			Object lastTDt = user.get(Constants.FAMOUS_NEWEST_TWEET_DATE);
//			Date lastTweetDtFromDb = lastTDt==null?null:(Date)lastTDt;
//			return lastTweetDtFromDb;
//		}
		return null;
	}
	

	@Override
	public FetchDatum fetchDatum(FetchDatum datum) {
		return datum;
	}
	

	/**
	 * 
	 * @param datum
	 * @param page
	 * @param listenList
	 * @return
	 * @author qiumm
	 */
	private List<FetchDatum> getTweetsList(String name,  List<FetchDatum> tweetsList, Date lastTweetDtFromDb) {
		if(name.equals("")){
			throw new RuntimeException("user name is empty");
		}
		String page = "0";//pageflag 0:first page,1:next page ,2:previous page
		String pagetime = "0";
		String lastid = "0";
		int exceptionNum = 0;
		boolean finFlag = false;
		while(!finFlag){
			String content = null;
			try {
				//此API可以返回tweet详细信息，但经常会有timeout错误
				content = statusAPI.userTimeline(oauth, format, page, pagetime,lastid, reqnum, name, "0", "0", "0");
				//此API一般不会又timeout错误，但不会有tweet详细信息，仅包含id等简单字段
//				content = statusAPI.user_timeline_ids(oauth, format, page, pagetime, reqnum, lastid, name, "0", "0", "0");
			} catch (Exception e) {
				this.await(1000);
				exceptionNum++;
				if(exceptionNum>30){
//					String[] emailAddress = new String[1];
//					emailAddress[0] = "qiumm820@gmail.com";
//					EmailUtil.send(emailAddress, "Tencent Exception", "internet has something wrong when crawl name:["+name+"].");
					this.await(60000);
					return tweetsList;
				}
				continue;
			}
			RawContent rawContent = new RawContent(content);
			ParseResult result = tweetsParse.parseList(rawContent);
			if(result == null){
				throw new RuntimeException("fetch content is empty");
			}
			Map<?, ?> metadata = result.getMetadata();
			if(metadata!=null){
				String errcode = StringUtils.valueOf(metadata.get("errcode"));
				if(!errcode.equals("0")){
					exceptionNum++;
					if(exceptionNum>10){
						String msg = StringUtils.valueOf(metadata.get("msg"));
						String ret = StringUtils.valueOf(metadata.get("ret"));
						log.warn("fetch user`s tweetsList ret["+ret+"] error ["+errcode+"]:"+msg);
						return tweetsList;
					}
					continue;
				}
				List<FetchDatum> pageTweetsList = result.getFetchList();
				//  check have crawled 
				for(FetchDatum tweetDatum:pageTweetsList){
					// check if lastid in the DB
					if (null!=lastTweetDtFromDb) {
						Date lastDate = (Date)tweetDatum.getMetadata().get("crtdt");
						if(!lastTweetDtFromDb.before(lastDate)){
							finFlag = true;
							break;
						}
					}
					tweetsList.add(tweetDatum);
				}
				String hasnext = StringUtils.valueOf(metadata.get("hasnext"));
				if(hasnext.equals("1")){//表示下页没有数据
					page = "1";
					lastid =StringUtils.valueOf(metadata.get("lastid"));
					pagetime =StringUtils.valueOf(metadata.get("pagetime"));
					if(pageTweetsList.size()<=1){
						break;
					}
					continue;
				}
				page = "1";
				lastid =StringUtils.valueOf(metadata.get("lastid"));
				pagetime =StringUtils.valueOf(metadata.get("pagetime"));
			}
			exceptionNum = 0;
		}
		return tweetsList;
	}
	
	
	/**
	 * move to next crawl instance
	 * @param curUser 
	 */
	protected void moveNext(Map<String, Object> curUser) {
		if(users.size()<=0){
			synchronized(this){
				if(users.size()<=0){
					List<Map<String, Object>> tops = crawlDB.queryQueue(crawlerQueueTableName,topN,Integer.valueOf(lastId));
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
		List<Map<String, Object>> list = crawlDB.queryQueue(crawlerQueueTableName,topN,Integer.valueOf(lastId));
		if(list==null || list.size()==0){
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
					meta.put(NICK, splits[1]);
					meta.put(DEPTH, "0");
					list.add(meta);
				}
			} catch (IOException e) {
				throw new RuntimeException(e);
			}
			appendQueueUsers(list);
		}
		users = new ArrayBlockingQueue<Map<String,Object>>(topN*2);
		users.addAll(list);
	}
	
	private List<Map<String, Object>> appendQueueUsers(List<Map<String, Object>> list) {
		for(Map<String, Object> item:list){
			item.put("key", item.get("name"));
		}
		this.crawlDB.insertQueueObjects(list);
		return list;
	}

	@Override
	protected void moveNext() {
		
	}
	
	public void datumFinish(FetchDatum datum){
		String lastUserId = datum.getMeta("lastUserId");
		if(StringUtils.isNotEmpty(lastUserId)){
			this.state.updateCurrentFetchState(lastUserId);
		}
	}
}
