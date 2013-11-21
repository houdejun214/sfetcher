package com.sdata.component.fetcher;

import java.io.IOException;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.httpclient.HttpStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.lakeside.core.utils.StringUtils;
import com.mongodb.DBObject;
import com.mongodb.MongoException;
import com.sdata.component.data.dao.FamousMgDao;
import com.sdata.component.data.dao.TweetsMgDao;
import com.sdata.component.parser.TencentFamousParser;
import com.sdata.component.parser.TencentTweetsParser;
import com.sdata.component.parser.TencentUserRelationParser;
import com.sdata.component.parser.TencentUserTweetsParser;
import com.sdata.core.Configuration;
import com.sdata.core.Constants;
import com.sdata.core.FetchDatum;
import com.sdata.core.NegligibleException;
import com.sdata.core.RawContent;
import com.sdata.core.RunState;
import com.sdata.core.fetcher.SdataFetcher;
import com.sdata.core.http.HttpCustomStatus;
import com.sdata.core.http.HttpPage;
import com.sdata.core.http.HttpPageLoader;
import com.sdata.core.parser.ParseResult;
import com.sdata.core.parser.SdataParser;
import com.sdata.core.util.EmailUtil;
import com.tencent.weibo.api.FriendsAPI;
import com.tencent.weibo.api.StatusesAPI;
import com.tencent.weibo.api.UserAPI;
import com.tencent.weibo.constants.OAuthConstants;
import com.tencent.weibo.oauthv1.OAuthV1;
import com.tencent.weibo.utils.Tencent;

/**
 * fetch user tweets of Tencent weibo
 * 
 * the fetcher use multiple-threads to fetch list of a user' tweets 
 * 
 * @author qmm
 *
 */
public class TencentFamousFetcher extends SdataFetcher{
	
	private static final Logger log = LoggerFactory.getLogger("SdataCrawler.TencentFamousFetcher");
	private Boolean isComplete = false;
	private StatusesAPI statusAPI;
	private FriendsAPI friendAPI;
	private UserAPI userAPI;
	private OAuthV1 oauth;
	
	private static final String format = "json";
	private static final String reqnum = "30";//每次请求的users条数,最大30条
	
	private SdataParser tweetsParse;
	private SdataParser userParse;
	private SdataParser relationParse;
	
	private TweetsMgDao tweetdao;
	private FamousMgDao famousdao;
	
	private String userName;
	private String passWd;
	
	private Date lastTweetDtFromDb;
	private Date lastTweetDt2Db;
	private Map<String,String> header;
	
	private static final String famousUrl = "http://zhaoren.t.qq.com/affectRank.php?id=0&timegap=day";

	public TencentFamousFetcher(Configuration conf,RunState state) throws UnknownHostException, MongoException {
		this.setConf(conf);
		this.setRunState(state);
		this.parser = new TencentFamousParser(conf,state);
		String host = this.getConf("mongoHost");
		int port = this.getConfInt("mongoPort",27017);
		String dbName = this.getConf("mongoDbName");
		this.tweetdao = new TweetsMgDao();
		this.tweetdao.initilize(host, port, dbName);
		this.famousdao = new FamousMgDao();
		this.famousdao.initilize(host, port, dbName);
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
		statusAPI = new StatusesAPI(OAuthConstants.OAUTH_VERSION_1);
		userAPI = new UserAPI(OAuthConstants.OAUTH_VERSION_1);
		tweetsParse = new TencentUserTweetsParser(this.getConf(),state);
		userParse = new TencentTweetsParser(this.getConf(),state);
		relationParse = new TencentUserRelationParser(this.getConf(), state);
		this.userName = conf.get("userName");
		this.passWd = conf.get("passWd");
		this.getCookie();
	}
	
	@Override
	public void taskInitialize() {
		isComplete = false;
		this.getCookie();
	}

	/**
	 * this method will be call with multiple-thread
	 * 
	 */
	@Override
	public List<FetchDatum> fetchDatumList() {
		List<FetchDatum> list = new ArrayList<FetchDatum>();
		for(int i=0;i<2;i++){
			int page = i+1;
			String url = famousUrl+"&p="+page;
			HttpPage hp = HttpPageLoader.getDefaultPageLoader().download(header,url);
			if(hp.getStatusCode() != HttpStatus.SC_OK){
				throw new NegligibleException(HttpCustomStatus.getStatusDescription(hp.getStatusCode())+url);
			}
			RawContent c = new RawContent(hp.getContentHtml());
			ParseResult result = parser.parseList(c);
			list.addAll(result.getFetchList());
		}
		log.info("******************************crawler famous list :"+list.size()+"********************************");
		isComplete = true;
		return list;
	}
	

	@Override
	public FetchDatum fetchDatum(FetchDatum datum) {
		String name = datum.getName();
		lastTweetDtFromDb = null;
		this.getLastTweetDt(name);
		List<Map<String,Object>> tweetsList = new ArrayList<Map<String,Object>>();
		tweetsList = this.getTweetsList(name, tweetsList);
		if(tweetsList.size()==0){
			lastTweetDt2Db = lastTweetDtFromDb;
		}else{
			Map<String,Object> newestTweetMap = tweetsList.get(0);
			Object id = newestTweetMap.get(Constants.TWEET_ID);
//			Integer crtl = (Integer)newestTweetMap.get("crtdt");
//			lastTweetDt2Db = DateTimeUtils.getTimeFromUnixTime(Long.valueOf(crtl));
			lastTweetDt2Db = (Date)(newestTweetMap.get("crtdt"));
			datum.addMetadata(Constants.FAMOUS_NEWEST_TWEET, id);
		}
		datum.addMetadata(Constants.FAMOUS_TWEETS, tweetsList);
		datum.addMetadata(Constants.FAMOUS_NEWEST_TWEET_DATE, lastTweetDt2Db);
		
		Map<String,Object> user = this.fetchUserMeta(name);
		datum.addMetadata(Constants.FAMOUS_ACCOUNT, user);
		datum.addMetadata(Constants.FAMOUS_FRENDC, user.get(Constants.FAMOUS_IDOLNUM));
		datum.addMetadata(Constants.FAMOUS_FOLC, user.get(Constants.FAMOUS_FANSNUM));
		datum.addMetadata(Constants.FAMOUS_STATC, user.get(Constants.FAMOUS_TWEETNUM));
		//getIdolList
		List<Map<String,Object>> IdolList = new ArrayList<Map<String,Object>>();
		IdolList = this.getIdolList(name, IdolList);
		datum.addMetadata(Constants.FAMOUS_IDOLLIST, IdolList);
		return datum;
	}
	
	private void getLastTweetDt(String name){
		DBObject famous =famousdao.queryByName(name);
		if(null!=famous){
			Object lastTDt = famous.get(Constants.FAMOUS_NEWEST_TWEET_DATE);
			lastTweetDtFromDb = lastTDt==null?null:(Date)lastTDt;
		}
	}
	
	/**
	 * 获取用户的关注列表
	 * @param datum
	 * @param page
	 * @param IdolList
	 * @return
	 * @author qiumm
	 */
	@SuppressWarnings("unchecked")
	private List<Map<String, Object>> getIdolList(String name,  List<Map<String,Object>> IdolList) {
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
					if(StringUtils.isNotEmpty(content)){
						break;
					}else{
						this.await(2000);
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
			ParseResult result = relationParse.parseSingle(rawContent);
			if(result == null){
				log.warn("fetch [{}]'s friends content[{}] is empty",name,content);
				return IdolList;
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
				return IdolList;
			}
			
			List<Map<String,Object>> usersList = (List<Map<String,Object>>)metadata.get(Constants.USER);
			IdolList.addAll(usersList);
			String hasnext = StringUtils.valueOf(metadata.get("hasnext"));
			if(hasnext.equals("1")){ //表示下页没有数据
				break;
			}
			page++;
		}
		//log.info("fetch user["+name+"]'s listen list count:"+listenList.size());
		return IdolList;
	}
	
	private Map<String,Object> fetchUserMeta(String name){
		String userJson = null;
		int exceptionNum = 0;
		boolean sucFlag = true;
		while(sucFlag){
			try {
				userJson = userAPI.otherInfo(oauth, format, name,null);
				if(StringUtils.isNotEmpty(userJson)){
					sucFlag = false;
				}else{
					this.await(2000);
				}
			} catch (Exception e) {
				exceptionNum++;
				if(exceptionNum>10){
					String[] emailAddress = new String[1];
					emailAddress[0] = "qiumm820@gmail.com";
					EmailUtil.send(emailAddress, "Tencent Exception in tencentFamous", "internet has something wrong when crawl name:["+name+"].");
					break;
				}
				continue;
			}
		}
		//此处使用TencentTweetsParser类的parseSingle方法，专门parser用户信息
		ParseResult parseResult = userParse.parseSingle(new RawContent(userJson));
		Map<?, ?> userMap = parseResult.getMetadata();
		Map<?, ?> userInfoMap = (Map<?, ?>)userMap.get(Constants.TENCENT_USER);
		String errcode =StringUtils.valueOf( userInfoMap.get("rerrcode"));
		if(!errcode.equals("0")){
			String msg = StringUtils.valueOf( userInfoMap.get("rmsg"));
			log.error("fetch tencent tweets 【"+name+"】 has error:【"+ msg +"】+"+"【"+errcode+"】");
		}
		userInfoMap.remove(Constants.TENCENT_USER_TWEETINFO);
		Map<String, Object> map = ((Map<String, Object>) userInfoMap);
		return map;
	}
	

	/**
	 * 
	 * @param datum
	 * @param page
	 * @param listenList
	 * @return
	 * @author qiumm
	 */
	private List<Map<String,Object>> getTweetsList(String name,  List<Map<String,Object>> tweetsList) {
		if(name.equals("")){
			throw new RuntimeException("user name is empty");
		}
		String page = "0";//pageflag 0:first page,1:next page ,2:previous page
		String pagetime = "0";
		String lastid = "0";
		int exceptionNum = 0;
		boolean finFlag = false;
		while (!finFlag) {
			String content = null;
			try {
				// 此API可以返回tweet详细信息，但经常会有timeout错误
				content = statusAPI.userTimeline(oauth, format, page, pagetime,
						lastid, reqnum, name, "0", "0", "0");
				// 此API一般不会又timeout错误，但不会有tweet详细信息，仅包含id等简单字段
				// content = statusAPI.userTimelineIds(oauth, format, page,
				// pagetime, reqnum, lastid, name, "0", "0", "0");
			} catch (Exception e) {
				System.out.println("catch exception"+e);
				exceptionNum++;
				if (exceptionNum > 10) {
					String[] emailAddress = new String[1];
					emailAddress[0] = "qiumm820@gmail.com";
					EmailUtil.send(emailAddress,
							"Tencent Exception in tencentFamous",
							"internet has something wrong when crawl name:["
									+ name + "].");
					return tweetsList;
				}
				continue;
			}
			exceptionNum = 0;
			RawContent rawContent = new RawContent(content);
			ParseResult result = tweetsParse.parseList(rawContent);
			if (result == null) {
				throw new RuntimeException("fetch content is empty");
			}
			Map<?, ?> metadata = result.getMetadata();
			if (metadata != null) {
				String errcode = StringUtils.valueOf(metadata.get("errcode"));
				if (!errcode.equals("0")) {
					exceptionNum++;
					if (exceptionNum > 10) {
						String msg = StringUtils.valueOf(metadata.get("msg"));
						String ret = StringUtils.valueOf(metadata.get("ret"));
						log.warn("fetch user`s tweetsList ret[" + ret
								+ "] error [" + errcode + "]:" + msg);
						return tweetsList;
					}
					continue;
				}
				List<FetchDatum> pageTweetsList = result.getFetchList();
				// check have crawled
				for (FetchDatum tweetDatum : pageTweetsList) {
					// check if lastid in the DB
					if (null!=lastTweetDtFromDb) {
						Date lastDate = (Date)tweetDatum.getMetadata().get("crtdt");
						if(!lastTweetDtFromDb.before(lastDate)){
							finFlag = true;
							break;
						}
					}
					tweetsList.add(tweetDatum.getMetadata());
				}
				String hasnext = StringUtils.valueOf(metadata.get("hasnext"));
				if (hasnext.equals("1")) {// 表示下页没有数据
					page = "1";
					lastid = StringUtils.valueOf(metadata.get("lastid"));
					pagetime = StringUtils.valueOf(metadata.get("pagetime"));
					finFlag = true;
					if (pageTweetsList.size() <= 1) {
						break;
					}
					continue;
				}
				page = "1";
				lastid = StringUtils.valueOf(metadata.get("lastid"));
				pagetime = StringUtils.valueOf(metadata.get("pagetime"));
			}
			exceptionNum = 0;
		}
		return tweetsList;
	}
	
	
	/**
	 * @param args
	*/
	public void getCookie() {
		header = new HashMap<String, String>();
		int i = 0;
		while(i<3){
			try {
				String cookie = Tencent.login(userName, passWd);
				header.put("Cookie", cookie);
				break;
			} catch (Exception e) {
				i++;
				e.printStackTrace();
				try {
					Thread.sleep(30*1000);
				} catch (InterruptedException e1) {
					// TODO Auto-generated catch block
					e1.printStackTrace();
				}
			}
		}
	}
	
	
	@Override
	public boolean isComplete(){
		return isComplete;
	}
	
	@Override
	protected void moveNext() {
		
	}
	
	public void datumFinish(FetchDatum datum){
	}
}
