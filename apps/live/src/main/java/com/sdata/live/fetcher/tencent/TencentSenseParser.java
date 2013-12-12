package com.sdata.live.fetcher.tencent;

import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.framework.db.hbase.thrift.HBaseClient;
import com.framework.db.hbase.thrift.HBaseClientFactory;
import com.lakeside.core.utils.StringUtils;
import com.lakeside.download.http.HttpPageLoader;
import com.sdata.context.config.Configuration;
import com.sdata.context.config.Constants;
import com.sdata.core.FetchDatum;
import com.sdata.core.RawContent;
import com.sdata.core.item.CrawlItemEnum;
import com.sdata.core.parser.ParseResult;
import com.sdata.sense.SenseFetchDatum;
import com.sdata.sense.item.SenseCrawlItem;
import com.sdata.sense.parser.SenseParser;
import com.tencent.weibo.api.UserAPI;
import com.tencent.weibo.beans.OAuth;
import com.tencent.weibo.oauthv1.OAuthV1;
import com.tencent.weibo.utils.Tencent;


/**
 * @author zhufb
 *
 */
public abstract class TencentSenseParser extends SenseParser{
	
	
	protected static final Logger log = LoggerFactory.getLogger("SdataCrawler.TencentSenseFrom");
	protected String format = "json";//返回数据的格式（json或xml）
	protected String pagesize = "30";//每页大小（1-30个）
	protected String contenttype = "0";//消息的正文类型（按位使用）0-所有，0x01-纯文本，0x02-包含url，0x04-包含图片，0x08-包含视频，0x10-包含音频 
	protected String sorttype = "0";//排序方式 0-表示按默认方式排序(即时间排序(最新)) 
	protected String msgtype = "0";//消息的类型（按位使用）0-所有，1-原创发表，2 转载，8-回复(针对一个消息，进行对话)，0x10-空回(点击客人页，进行对话) 
	protected static  HBaseClient client;
	protected OAuth oauth;
	protected Map<String,String> header = new HashMap<String,String>();
	protected static HttpPageLoader pageLoader = HttpPageLoader.getAdvancePageLoader();
	public TencentSenseParser(Configuration conf) {
		super(conf);
	}
	
	public TencentSenseParser(Configuration conf,OAuth v,Map<String,String> header){
		super(conf);
		this.oauth = v;
		this.header = header;
	}

	public static TencentSenseParser getTencentSenseFrom(Configuration conf ,SenseCrawlItem item){
		
		//Oauth 2
//		String ClientId = conf.get("ClientId");
//		String OpenId = conf.get("OpenId");
//		String AccessToken = conf.get("AccessToken");
//		OAuthV2 oauth = new OAuthV2();
//
//		oauth.setClientId(ClientId);
//		oauth.setOpenid(OpenId);
//		oauth.setOauthVersion(OAuthConstants.OAUTH_VERSION_2_A);
//		oauth.setAccessToken(AccessToken);

		//Oauth 1
		OAuthV1 oauth = new OAuthV1();
		String consumerKey = conf.get("ConsumerKey");
		String consumerSecret = conf.get("ConsumerSecret");
		String AccessToken = conf.get("AccessToken");
		String AccessTokenSecret = conf.get("AccessTokenSecret");
		oauth.setOauthConsumerKey(consumerKey);
		oauth.setOauthConsumerSecret(consumerSecret);
		oauth.setOauthToken(AccessToken);
		oauth.setOauthTokenSecret(AccessTokenSecret);
		
		String clusterName = conf.get("hbase.cluster.name");
		String namespace = conf.get("hbase.namespace");
		client = HBaseClientFactory.getClientWithCustomSeri(clusterName, namespace);
		if(item.containParam(CrawlItemEnum.KEYWORD.getName())){
			return new TencentSenseParserWord(conf,oauth,getHttpHeader(conf));
		}else if(item.containParam(CrawlItemEnum.ACCOUNT.getName())){
			return new TencentSenseParserUser(conf,oauth,getHttpHeader(conf));
		}else{
			throw new RuntimeException("wrong word type input!");
		}
	}
	

	protected static Map<String,String> getHttpHeader(Configuration conf){
		Map<String,String> header = new HashMap<String,String>();
		String qq = conf.get("qq","1305327854");
		String password = conf.get("password","nusnext");
		String cookie = conf.get("cookie");
		if(StringUtils.isEmpty(cookie)){
			cookie = Tencent.getCookie(qq, password);
		}
		header.put("Cookie", cookie);
		return header;
	}
	
	protected Map<String, Object> fetchUserInfo(String name,UserAPI userAPI,OAuth oauth,TencentJsonParser parser) {
		String userJson = null;
		while (true) {
			try {
				userJson = userAPI.otherInfo(oauth, "json", name, null);
				if(StringUtils.isEmpty(userJson)){
					continue;
				}else{
					ParseResult parseResult = parser.parseUserInfo(new RawContent(userJson));
					Map metadata = parseResult.getMetadata();
					Map userInfoMap = (Map)metadata.get(Constants.TENCENT_USER);
					String errcode =StringUtils.valueOf( userInfoMap.get("rerrcode"));
					if(!errcode.equals("0")){
						String msg = StringUtils.valueOf( userInfoMap.get("rmsg"));
						log.error("*********** tencent user 【"+name+"】 has error:【"+ msg +"】");
						return null;
					}
					userInfoMap.remove(Constants.TENCENT_USER_TWEETINFO);
					return (Map<String, Object>)userInfoMap;
				}
			} catch (Exception e) {
				continue;
			}
			
		}
	}
	
	public String getUnixTime(Date date) {
		if(date == null){
			return "";
		}
		Calendar instance = Calendar.getInstance();
		instance.setTime(date);
		return StringUtils.valueOf(instance.getTimeInMillis() / 1000);
	}
	

	protected void await(long millis){
		try {
			Thread.sleep(millis);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

	public abstract List<FetchDatum> getList(SenseCrawlItem item,TencentCrawlState state);
	
	public abstract void next(TencentCrawlState state);
	
	public abstract Map<String,Object> getTencentUserInfo(String name);

	public abstract boolean isComplete();
	
	public abstract SenseFetchDatum getDatum(SenseFetchDatum datum);
	
}
