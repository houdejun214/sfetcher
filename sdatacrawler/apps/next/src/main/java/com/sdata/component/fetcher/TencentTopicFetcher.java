package com.sdata.component.fetcher;

import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.lakeside.core.utils.StringUtils;
import com.sdata.component.parser.TencentTopicParser;
import com.sdata.component.parser.TencentTweetsParser;
import com.sdata.component.parser.TencentUserTweetsParser;
import com.sdata.component.site.TopicAPI;
import com.sdata.core.Configuration;
import com.sdata.core.Constants;
import com.sdata.core.FetchDatum;
import com.sdata.core.RawContent;
import com.sdata.core.RunState;
import com.sdata.core.fetcher.SdataFetcher;
import com.sdata.core.parser.ParseResult;
import com.sdata.core.parser.SdataParser;
import com.sdata.core.util.WebPageDownloader;
import com.tencent.weibo.api.TAPI;
import com.tencent.weibo.api.TrendsAPI;
import com.tencent.weibo.api.UserAPI;
import com.tencent.weibo.constants.OAuthConstants;
import com.tencent.weibo.oauthv1.OAuthV1;
import com.tencent.weibo.utils.Tencent;

public class TencentTopicFetcher extends SdataFetcher{
	
	private static final Logger log = LoggerFactory.getLogger("SdataCrawler.TencentTopicFetcher");
	
	private TrendsAPI trendsAPI;
	private UserAPI userAPI;
	private TAPI tAPI;
	
	private String topicType = "3";
	private String topicReqnum;
	private String pos = "0";
	private OAuthV1 oauth;
	private String consumerKey;
	private String consumerSecret;
	private String AccessToken;
	private String AccessTokenSecret;
	private String format = "json";
	
	private TopicAPI topicAPI;

	private SdataParser userParse;
	private SdataParser tweetParse;
	
	private String topicInfoBaseUrl = "http://k.t.qq.com/k/";
	
	private int hotTweetsReqPageNum;
	private int latestTweetsReqPageNum;
	
	private Map<String,String> header;
	private String userName;
	private String passWd;
	
	public TencentTopicFetcher(Configuration conf,RunState state) {
		this.setConf(conf);
		this.setRunState(state);
		topicReqnum = conf.get("topicReqnum");
		this.parser = new TencentTopicParser(conf,state);
		consumerKey = conf.get("ConsumerKey");
		hotTweetsReqPageNum = conf.getInt("hotTweetsReqPageNum",10);
		latestTweetsReqPageNum = conf.getInt("latestTweetsReqPageNum",10);
		consumerSecret = conf.get("ConsumerSecret");
		AccessToken = conf.get("AccessToken");
		AccessTokenSecret = conf.get("AccessTokenSecret");
		this.userName = conf.get("userName");
		this.passWd = conf.get("passWd");
		oauth = new OAuthV1();
		oauth.setOauthConsumerKey(consumerKey);
		oauth.setOauthConsumerSecret(consumerSecret);
		oauth.setOauthToken(AccessToken);
		oauth.setOauthTokenSecret(AccessTokenSecret);
		trendsAPI = new TrendsAPI(OAuthConstants.OAUTH_VERSION_1);
		tAPI = new TAPI(OAuthConstants.OAUTH_VERSION_1);
		userAPI = new UserAPI(OAuthConstants.OAUTH_VERSION_1);
		userParse = new TencentTweetsParser(this.getConf(),state);
		tweetParse = new TencentUserTweetsParser(this.getConf(),state);
		topicAPI = new TopicAPI(conf,state);
	}

	@Override
	public List<FetchDatum> fetchDatumList() {
		List<FetchDatum> resultList = new ArrayList<FetchDatum>();
		String content = null;
		try {
			content = trendsAPI.ht(oauth, format, topicType, topicReqnum, pos);
			if (content == null || content.equals("")) {
				log.error("******************************** t.qq.com visit abnormal *************************************");
			}else{
				ParseResult parseResult = parser.parseList(new RawContent(content));
				String errcode =StringUtils.valueOf( parseResult.getMetadata().get("errcode"));
				if(!errcode.equals("0")){
					String msg = StringUtils.valueOf( parseResult.getMetadata().get("msg"));
					log.error("*********** tencent topics fetchDatumList() has error:【"+ msg +"】");
				}
				List<FetchDatum> fetchList = parseResult.getFetchList();
				resultList.addAll(fetchList);
			}
			log.info("system has fetched topics count:{}",resultList.size());
			resultList = topicAPI.parseTencentTopic(resultList);
			log.info("will fetch info topics count:{}",resultList.size());
		} catch (Exception e) {
			e.printStackTrace();
		}
		this.getCookie();
		return resultList;
	}
	
	
	@Override
	public FetchDatum fetchDatum(FetchDatum datum) {
		if(header == null||header.size() == 0){
			return null;
		}
		List<Map<String,Object>> topicTweets = new ArrayList<Map<String,Object>>();
		String content = null;
		Map<String,Object> meta = datum.getMetadata();
		//first:fetch topic info
		String topicKeywords = StringUtils.valueOf(meta.get("keywords"));
		String topicInfoUrl = topicInfoBaseUrl + URLEncoder.encode(topicKeywords);
		content = this.requestUrlWithRepeat(topicInfoUrl, 20, "tencent topic URL can't visit:"+topicInfoUrl,header);
		if(content!=null){
			Map<String,Object> topicInfoMap = ((TencentTopicParser)parser).parseTopicInfo(content,topicInfoUrl);
			if(topicInfoMap!=null){
				meta.put(Constants.TOPIC_URL, topicInfoUrl);
				meta.put(Constants.TOPIC_DESCRIPTION, topicInfoMap.get(Constants.TOPIC_DESCRIPTION));
				meta.put(Constants.TOPIC_IMG, topicInfoMap.get(Constants.TOPIC_IMG));
				meta.put(Constants.TOPIC_COUNT, topicInfoMap.get(Constants.TOPIC_COUNT));
			}
		}
		//second:fetch topic's tweets
		content = null;
		//fetch hot tweets
		String sortByHot = "2";//sort by hot
		Map<String,String> repeatCheck = new HashMap<String,String>();
		List<Map<String,Object>> hotTweets = this.fetchTopicsTweet(topicInfoUrl, sortByHot, hotTweetsReqPageNum, repeatCheck,topicKeywords);
		topicTweets.addAll(hotTweets);
		//fetch latest tweets
		String sortByTime = "1";//sort by latest
		List<Map<String,Object>> latestTweets = this.fetchTopicsTweet(topicInfoUrl, sortByTime, latestTweetsReqPageNum, repeatCheck,topicKeywords);
		topicTweets.addAll(latestTweets);
		log.info("Topic["+topicKeywords+"] has crawled ["+topicTweets.size()+"] tweets.");
		if(topicTweets.size()==0){
			String state = StringUtils.valueOf(meta.get(Constants.TOPIC_STATE));
			if(!Constants.TOPIC_STATE_END.equals(state)){
				datum.clearMetadata();
				return datum;
			}
		}
		datum.addMetadata(Constants.TOPIC_TWEETS, topicTweets);
		return datum;
	}
	
	/**
	 * get tweets in the topic by different sort type with the max page num limit
	 * @param url
	 * @param sortType 1:sort by time desc,2:sort by hot
	 * @param maxPageCount :how many pages will get
	 * @return
	 * @author qiumm
	 */
	private List<Map<String,Object>> fetchTopicsTweet(String url,String sortType,int maxPageCount,Map<String,String> repeatCheck,String topicName){
		List<Map<String,Object>> topicTweets = new ArrayList<Map<String,Object>>();
		String fetchUrl = url + "?sort=" + sortType;
		int pageCount = 0;
		int tweetInPageCount = 0;
		int tweetCrawlCount = 0;
		while(pageCount<maxPageCount && StringUtils.isNotEmpty(fetchUrl)){
			pageCount++;
			String content = this.requestUrlWithRepeat(fetchUrl, 100, "fetch topic tweets URL can't visit:"+fetchUrl,header);
			if(content!=null){
				Map<String,Object> topicTweetsInfoMap = ((TencentTopicParser)parser).parseTopicTweetsPage(content);
				List<String> tweetidsList = (List<String>)topicTweetsInfoMap.get("tweetidsList");
				for(int i=0;i<tweetidsList.size();i++){
					String tweetId = tweetidsList.get(i);
					if(repeatCheck.containsKey(tweetId)){
						continue;
					}
					repeatCheck.put(tweetId, tweetId);
					tweetInPageCount++;
					Map<String,Object> tweetObj = this.getOneTweet(tweetId);
					if(tweetObj!=null){
						if(!tweetObj.containsKey(Constants.OBJECT_ID)){
							log.info(" fetch tweet info by tweetid["+tweetId+"] don't have _id,the info is:"+tweetObj.toString());
						}else{
							if(tweetObj.containsKey("user")){
								Map<String,Object> user = (Map<String,Object>)tweetObj.get("user");
								if(user.containsKey("_id")){
									topicTweets.add(tweetObj);
									tweetCrawlCount++;
								}
//							else{
//								log.info(" fetch tweet info by tweetid["+tweetId+"] don't have userinfo,the tweet info is:"+tweetObj.toString());
//							}
							}
							
						}
					}
				}
				String nextPageUrl = (String) topicTweetsInfoMap.get("nextPageUrl");
				fetchUrl = nextPageUrl;
			}
		}
		log.error("get tweets from page count["+tweetInPageCount+"] by page["+pageCount+"] sort["+sortType+"] topic["+topicName+"]");
		log.error("carwl tweets from api count["+tweetCrawlCount+"] by page["+pageCount+"] sort["+sortType+"] topic["+topicName+"]");
		return topicTweets;
	}
	
	/**
	 * get tweet's info by tweetid
	 * @param tweetId
	 * @return
	 * @author qiumm
	 */
	private Map<String,Object> getOneTweet(String tweetId){
		Map<String,Object> tweetObj = new HashMap<String,Object>();
		int repeatCount = 0;
		String content = null;
		while(repeatCount<=20){
			repeatCount++;
			try {
				content = tAPI.show(oauth, "json", tweetId);
			} catch (Exception e) {
				continue;
			}
			if(content==null || content.equals("")){
				continue;
			}else{
				break;
			}
		}
		if(content==null || content.equals("")){
			log.info("can't fetch the tweet by tweetid["+tweetId+"]");
			return null;
		}
		RawContent rawContent = new RawContent(content);
		ParseResult result = tweetParse.parseSingle(rawContent);
		if(result == null){
			return null;
		}
		tweetObj = result.getMetadata();
		if(tweetObj!=null){
			String errcode = StringUtils.valueOf(tweetObj.get("errcode"));
			if(!errcode.equals("0")){
				return null;
			}else{
				String username = StringUtils.valueOf(tweetObj.get("name"));
				String userJson = null;
				int repeatGetUserCount = 0;
				while(repeatGetUserCount<=20){
					repeatGetUserCount++;
					try {
						userJson = userAPI.otherInfo(oauth, "json", username,null);
					} catch (Exception e) {
						continue;
					}
					if(userJson==null || userJson.equals("")){
						continue;
					}else{
						//此处使用TencentTweetsParser类的parseSingle方法，专门parser用户信息
						ParseResult parseResult = userParse.parseSingle(new RawContent(userJson));
						Map userMap = parseResult.getMetadata(); 
						Map userInfoMap = (Map)userMap.get(Constants.TENCENT_USER);
						errcode =StringUtils.valueOf( userInfoMap.get("rerrcode"));
						if(errcode.equals("0")){
							userInfoMap.remove(Constants.TENCENT_USER_TWEETINFO);
							tweetObj.put(Constants.TENCENT_USER, userInfoMap);
						}else{
//							log.error("**********api crawl userinf["+username+"] error["+errcode+"]***********");
							continue;
						}
						break;
					}
				}
				if(userJson==null || userJson.equals("")){
					log.info("can't fetch the user info by user name["+username+"]");
					return null;
				}
			}
		}
		return tweetObj;
	}
	
	/**
	 * move to next crawl instance
	 */
	@Override
	protected void moveNext() {
	}
	
	@Override
	public boolean isComplete(){
		return false;
		
	}
	
	/**
	 * request a url ,if internet has something wrong,repeat try when repeat count is smaller than MaxRepeatCount
	 * @param url
	 * @param MaxRepeatCount
	 * @param errorInfo
	 * @return
	 * @author qiumm
	 */
	private String requestUrlWithRepeat(String url,int MaxRepeatCount,String errorInfo,Map<String,String> header){
		if(header == null){
			getCookie();
		}
		String content = null;
		int repeatCount = 0;
		while(repeatCount<=MaxRepeatCount){
			repeatCount++;
			try {
				content = WebPageDownloader.download(url,header);
			} catch (Exception e) {
				continue;
			}
			if(content==null || content.equals("")){
				continue;
			}else{
				break;
			}
		}
		if(content==null || content.equals("")){
			header = null;
			log.info(errorInfo);
		}
		return content;
	}
	
	/**
	 * @param args
	*/
	public synchronized void getCookie() {
		if(header != null&&header.containsKey("Cookie")){
			return;
		}
		
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
	
	
//	public void getCookie() {
//		header = new HashMap<String, String>();
//		HttpClient client = new DefaultHttpClient();
//		client.getParams().setParameter(HttpConnectionParams.CONNECTION_TIMEOUT, 5000);
//		try {
//			/********************* 获取验证码 ***********************/
//			HttpGet get = new HttpGet("http://check.ptlogin2.qq.com/check?uin="
//					+ userName + "&appid=46000101&ptlang=2052&r="
//					+ Math.random());
//			get.setHeader("Host", "check.ptlogin2.qq.com");
//			get.setHeader("Referer", "http://t.qq.com/?from=11");
//			get.setHeader("User-Agent",
//					"Mozilla/5.0 (Windows NT 5.1; rv:13.0) Gecko/20100101 Firefox/13.0");
//			HttpResponse response = client.execute(get);
//			String entity = EntityUtils.toString(response.getEntity());
//			String[] checkNum = entity
//					.substring(entity.indexOf("(") + 1, entity.lastIndexOf(")"))
//					.replace("'", "").split(",");
//			/******************** *加密密码 ***************************/
//			String pass = MD5Security.GetPassword(checkNum[2].trim(), passWd,
//					checkNum[1].trim());
//			/************************* 登录 ****************************/
//			get = new HttpGet(
//					"http://ptlogin2.qq.com/login?ptlang=2052&u="
//							+ userName
//							+ "&p="
//							+ pass
//							+ "&verifycode="
//							+ checkNum[1]
//							+ "&aid=46000101&u1=http%3A%2F%2Ft.qq.com&ptredirect=1&h=1&from_ui=1&dumy=&fp=loginerroralert&action=4-12-14683&g=1&t=1&dummy=");
//			get.setHeader("Connection", "keep-alive");
//			get.setHeader("Host", "ptlogin2.qq.com");
//			get.setHeader("Referer", "http://t.qq.com/?from=11");
//			get.setHeader("User-Agent",
//					"Mozilla/5.0 (Windows NT 5.1; rv:13.0) Gecko/20100101 Firefox/13.0");
//			response = client.execute(get);
//			entity = EntityUtils.toString(response.getEntity());
//			if (entity.indexOf("登录成功") > -1) {
//				Header[] headers = response.getHeaders("Set-Cookie");
//				String cookiestr = "";
//				for (Header header : headers) {
//					String cookie = header.toString()
//							.replace("Set-Cookie:", "").trim();
//					if (cookie.toUpperCase().contains("DELETED")) {
//						break;
//					}
//					cookiestr += cookie.substring(0, cookie.indexOf(";")) + ";";
//				}
//				header.put("Cookie", cookiestr);
//			}else{
//				log.info("user " + userName + " longin unsuccessed!" + entity);	
//			}
//		} catch (Exception e) {
//            	   e.printStackTrace();
//        }
//	}
	
//	/**
//	 * get user's info by tweetid
//	 * @param htid
//	 * @param getTweetsList
//	 * @author qiumm
//	 */
//	private void getUserInfoByTweets(String htid,
//			List<Map<String, Object>> getTweetsList) {
//		for(int i=0;i<getTweetsList.size();i++){
//			Map<String,Object> tweetObj = (Map<String,Object>)getTweetsList.get(i);
//			//下一次请求时会传入上次请求的最后一条微博的id，而下一次请求结果中会包含上次请求的最后一条微博，此时，须判断并删除。
//			String id = StringUtils.valueOf(tweetObj.get("id"));
//			if(id.equals(htid)){
//				getTweetsList.remove(i);
//				i--;
//				continue;
//			}
//			//fetch tweet's user info
//			String username = StringUtils.valueOf(tweetObj.get("name"));
//			String userJson = null;
//			int repeatGetUserCount = 0;
//			while(repeatGetUserCount<=10){
//				repeatGetUserCount++;
//				try {
//					userJson = userAPI.otherInfo(oauth, "json", username,null);
//				} catch (Exception e) {
//					e.printStackTrace();
//				}
//				if(userJson==null || userJson.equals("")){
//					log.error("******************************** t.qq.com visit abnormal *************************************");
//				}else{
//					//此处使用TencentTweetsParser类的parseSingle方法，专门parser用户信息
//					ParseResult parseResult = userParse.parseSingle(new RawContent(userJson));
//					Map userMap = parseResult.getMetadata(); 
//					Map userInfoMap = (Map)userMap.get("user");
//					String errcode =StringUtils.valueOf( userInfoMap.get("rerrcode"));
//					if(errcode.equals("0")){
//						tweetObj.put(Constants.TENCENT_USER, userMap.get(Constants.TENCENT_USER));
//						break;
//					}
//				}
//			}
//			//if we can't get the tweet's user info ,we delete the tweet
//			if(!tweetObj.containsKey(Constants.TENCENT_USER)){
//				log.error("*********** crawler can't fetch username 【"+username+"】 info.");
//				getTweetsList.remove(i);
//				i--;
//			}
//		}
//	}
}
