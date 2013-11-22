package com.sdata.sense.parser.tencentHot;

import java.util.Calendar;
import java.util.Date;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.framework.db.hbase.thrift.HBaseClient;
import com.framework.db.hbase.thrift.HBaseClientFactory;
import com.lakeside.core.utils.StringUtils;
import com.lakeside.data.redis.RedisDB;
import com.sdata.core.Configuration;
import com.sdata.core.Constants;
import com.sdata.core.RawContent;
import com.sdata.core.crawldb.CrawlDBRedis;
import com.sdata.core.parser.ParseResult;
import com.sdata.sense.SenseFetchDatum;
import com.sdata.sense.fetcher.tencent.TencentJsonParser;
import com.sdata.sense.item.SenseCrawlItem;
import com.sdata.sense.parser.SenseParser;
import com.tencent.weibo.api.StatusesAPI;
import com.tencent.weibo.api.UserAPI;
import com.tencent.weibo.constants.OAuthConstants;
import com.tencent.weibo.oauthv1.OAuthV1;

public class TencentHotParser extends SenseParser {

	protected static Logger log = LoggerFactory.getLogger("Sense.TencentHotParser");
	private static OAuthV1 oauth;
	private static StatusesAPI statusAPI;
	private static UserAPI userAPI;
	private String UID = "dtf_uid";
	private static final String format = "json";
	private TencentJsonParser tencentJsonParser;
	private static HBaseClient client;
	private static String tencentUserTable = "hot_tencent_users";
	private static Object syn = new Object();
	public TencentHotParser(Configuration conf) {
		super(conf);
		tencentJsonParser = new TencentHotJsonParser();
		String namespace = conf.get("hbase.namespace");
		String clusterName = conf.get("hbase.cluster.name");
		client = HBaseClientFactory.getClientWithCustomSeri(clusterName,namespace); 
	}

	@Override
	public ParseResult parseCrawlItem(Configuration conf, RawContent rc,
			SenseCrawlItem item) {
		ParseResult result = new ParseResult();
		TencentOAuthV1.init(conf);
		RedisDB redisDB = CrawlDBRedis.getRedisDB(conf,"TencentHot");
		String uid = StringUtils.valueOf(item.getParam(UID));
		String dbState = redisDB.get(uid);
		String page = "0";//pageflag 0:first page,1:next page ,2:previous page
		String lastid="0";
		String pagetime = "0";
		String reqnum = "1";
		if(!StringUtils.isEmpty(dbState)){
			String[] split = dbState.split(",");
			if(split.length == 2){
				lastid =split[0];
				pagetime = split[1];
				page = "2";
				reqnum = "100";
			}
		}
		String content = null;
		try {
			content = statusAPI.userTimeline(oauth, format, page, pagetime,
					lastid, reqnum, uid, "0", "0", "0");
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
		ParseResult pageResult = tencentJsonParser.parseTencentAPITweets(item, new RawContent(content));
		if (pageResult == null) {
			throw new RuntimeException("fetch content is empty");
		}
		Map<?, ?> metadata = pageResult.getMetadata();
		if (metadata == null||!"0".equals(StringUtils.valueOf(metadata.get("errcode")))) {
			throw new RuntimeException("error code is wrong!!");
		}
		result.getFetchList().addAll(pageResult.getFetchList());
		lastid = StringUtils.valueOf(metadata.get("lastid"));
		pagetime = StringUtils.valueOf(metadata.get("pagetime"));
		if(!StringUtils.isEmpty(lastid)&&!StringUtils.isEmpty(pagetime)){
			redisDB.set(uid, lastid+","+pagetime);
		}
		log.warn("fetch tencent uid:"+uid+",fetch tweets size:"+result.getListSize());
		return result;
	}
	
	@Override
	public SenseFetchDatum parseDatum(SenseFetchDatum datum,Configuration conf, RawContent c){
		String name = datum.getMeta(Constants.USER_NAME);
		if(!checkHaveUser(name)){
			Map<String, Object> userInfo = this.fetchUserInfo(name);
			if (userInfo != null) {
				userInfo.put(Constants.USER_ID, name);
				datum.addMetadata(Constants.TENCENT_USER_INFO, userInfo);
			}
		}
		return datum;
	}
	
	private long getUnixTime(Date date) {
		Calendar instance = Calendar.getInstance();
		instance.setTime(date);
		return instance.getTimeInMillis() / 1000;
	}
	
	private boolean checkHaveUser(String userId) {
		return client.exists(tencentUserTable, userId);
	}
	private Map<String, Object> fetchUserInfo(String name) {
		int i=0;
		while(i++<3){
			try {
				String userJson = userAPI.otherInfo(oauth, format, name, null);
				if (!StringUtils.isEmpty(userJson)) {
					ParseResult parseResult = tencentJsonParser
							.parseUserInfo(new RawContent(userJson));
					Map metadata = parseResult.getMetadata();
					Map userInfoMap = (Map) metadata.get(Constants.TENCENT_USER);
					String errcode = StringUtils.valueOf(userInfoMap
							.get("rerrcode"));
					if (!errcode.equals("0")) {
						String msg = StringUtils.valueOf(userInfoMap.get("rmsg"));
						log.error("*********** tencent user 【" + name+ "】 has error:【" + msg + "】");
						continue;
					}
					userInfoMap.remove(Constants.TENCENT_USER_TWEETINFO);
					return (Map<String, Object>) userInfoMap;
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		return null;
	}
	
	static class TencentOAuthV1{
		public static void init(Configuration conf){
			if(statusAPI == null){
				synchronized (syn) {
					if(statusAPI == null){
						String consumerKey = conf.get("ConsumerKey");
						String consumerSecret = conf.get("ConsumerSecret");
						String AccessToken = conf.get("AccessToken");
						String AccessTokenSecret = conf.get("AccessTokenSecret");
						oauth = new OAuthV1();
						oauth.setOauthConsumerKey(consumerKey);
						oauth.setOauthConsumerSecret(consumerSecret);
						oauth.setOauthToken(AccessToken);
						oauth.setOauthTokenSecret(AccessTokenSecret);
						userAPI = new UserAPI(OAuthConstants.OAUTH_VERSION_1);
						statusAPI = new StatusesAPI(OAuthConstants.OAUTH_VERSION_1);
					}
				}
			}
		}
	}

}
