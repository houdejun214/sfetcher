package com.sdata.component.fetcher;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import net.sf.json.JSONArray;
import net.sf.json.JSONObject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.lakeside.core.utils.StringUtils;
import com.sdata.component.parser.TencentTweetsParser;
import com.sdata.core.Configuration;
import com.sdata.core.Constants;
import com.sdata.core.FetchDatum;
import com.sdata.core.RawContent;
import com.sdata.core.RunState;
import com.sdata.core.fetcher.SdataFetcher;
import com.sdata.core.parser.ParseResult;
import com.tencent.weibo.api.StatusesAPI;
import com.tencent.weibo.api.UserAPI;
import com.tencent.weibo.constants.OAuthConstants;
import com.tencent.weibo.oauthv1.OAuthV1;

public class TencentTweetsFetcher extends SdataFetcher{
	
	private static final Logger log = LoggerFactory.getLogger("SdataCrawler.TencentTweetsFetcher");
	
	private String consumerKey;
	private String consumerSecret;
	private String AccessToken;
	private String AccessTokenSecret;
	private OAuthV1 oauth;
	
	private String pos;

	private StatusesAPI statusesAPI;
	private UserAPI userAPI;
	
	public TencentTweetsFetcher(Configuration conf,RunState state) {
		this.setConf(conf);
		this.setRunState(state);
		consumerKey = conf.get("ConsumerKey");
		consumerSecret = conf.get("ConsumerSecret");
		AccessToken = conf.get("AccessToken");
		AccessTokenSecret = conf.get("AccessTokenSecret");
		oauth = new OAuthV1();
		oauth.setOauthConsumerKey(consumerKey);
		oauth.setOauthConsumerSecret(consumerSecret);
		oauth.setOauthToken(AccessToken);
		oauth.setOauthTokenSecret(AccessTokenSecret);
		parser = new TencentTweetsParser(conf,state);
		statusesAPI = new StatusesAPI(OAuthConstants.OAUTH_VERSION_1);
		userAPI = new UserAPI(OAuthConstants.OAUTH_VERSION_1);
	}

	@Override
	public List<FetchDatum> fetchDatumList() {
		pos = "0";
		List<FetchDatum> fetchList = new ArrayList<FetchDatum>();
		try {
			int exceptionNum = 0;
			while(true){
				String respones = null;
				try {
					respones = statusesAPI.publicTimeline(oauth, "json", pos, "30");
					if(respones==null || respones.equals("")){
						exceptionNum++;
						continue;
					}
				} catch (Exception e) {
					exceptionNum++;
					if(exceptionNum>20){
						log.info("internet has something wrong when crawl tencent API:public_TimeLine.");
						break;
					}
					continue;
				}
				exceptionNum=0;
				
				ParseResult parseResult = parser.parseList(new RawContent(respones));
				
				String errcode =StringUtils.valueOf( parseResult.getMetadata().get("errcode"));
				if(!errcode.equals("0")){
					String msg = StringUtils.valueOf( parseResult.getMetadata().get("msg"));
					log.error("*********** tencent tweets fetchDatumList() has error:【"+ msg +"】");
					return fetchList;
				}
				String hasnext = StringUtils.valueOf( parseResult.getMetadata().get("hasnext"));
				if(!hasnext.equals("0")){//"0" mean yes
					break;
				}
				if(parseResult.isListEmpty()){
					String msg = StringUtils.valueOf( parseResult.getMetadata().get("msg"));
					if("have no tweet".equals(msg)){
						return fetchList;
					}
					continue;
				}
				fetchList.addAll(parseResult.getFetchList()); 
				System.out.println("************crawl tweets count:!" + fetchList.size()+"************");
				break;
			}
			return fetchList;
		} catch (Exception e) {
			e.printStackTrace();
		}
		return fetchList;
	}
	
	@Override
	public FetchDatum fetchDatum(FetchDatum datum) {
		Map tweetObj = datum.getMetadata();
		if(tweetObj.containsKey(Constants.TWEET_RETWEETED)){
			Map reTweetObj = (Map)tweetObj.get(Constants.TWEET_RETWEETED);
			reTweetObj = fetchUserInfo(reTweetObj);
			if(reTweetObj==null){
				tweetObj.remove(Constants.TWEET_RETWEETED);
			}else{
				tweetObj.put(Constants.TWEET_RETWEETED, reTweetObj);
			}
		}
		tweetObj = fetchUserInfo(tweetObj);
		if(tweetObj==null){
			datum = null;
		}
		return datum;
	}

	private Map fetchUserInfo(Map tweetObj) {
		String name = StringUtils.valueOf(tweetObj.get("name"));
		String userJson = null;
		int exceptionNum = 0;
		while (true) {
			try {
				userJson = userAPI.otherInfo(oauth, "json", name, null);
				if(StringUtils.isEmpty(userJson)){
					exceptionNum++;
					continue;
				}
				break;
			} catch (Exception e) {
				exceptionNum++;
				if(exceptionNum>20){
					log.info("internet has something wrong when crawl tencent homeTimeLine.");
					return null;
				}
				continue;
			}
		}
		ParseResult parseResult = parser.parseSingle(new RawContent(userJson));
		Map userMap = parseResult.getMetadata();
		Map userInfoMap = (Map)userMap.get(Constants.TENCENT_USER);
		String errcode =StringUtils.valueOf( userInfoMap.get("rerrcode"));
		if(!errcode.equals("0")){
			String msg = StringUtils.valueOf( userInfoMap.get("rmsg"));
			log.error("*********** tencent tweets 【"+name+"】 has error:【"+ msg +"】");
			return null;
		}
		userInfoMap.remove(Constants.TENCENT_USER_TWEETINFO);
		tweetObj.put(Constants.TENCENT_USER_INFO, userInfoMap);
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
}
