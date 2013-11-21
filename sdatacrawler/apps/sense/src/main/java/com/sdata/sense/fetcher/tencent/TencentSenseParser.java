package com.sdata.sense.fetcher.tencent;

import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.lakeside.core.utils.StringUtils;
import com.nus.next.db.hbase.thrift.HBaseClient;
import com.nus.next.db.hbase.thrift.HBaseClientFactory;
import com.sdata.core.Configuration;
import com.sdata.core.Constants;
import com.sdata.core.FetchDatum;
import com.sdata.core.RawContent;
import com.sdata.core.RunState;
import com.sdata.core.item.CrawlItemEnum;
import com.sdata.core.parser.ParseResult;
import com.sdata.sense.SenseFetchDatum;
import com.sdata.sense.item.SenseCrawlItem;
import com.sdata.sense.parser.SenseParser;
import com.tencent.weibo.api.UserAPI;
import com.tencent.weibo.constants.OAuthConstants;
import com.tencent.weibo.oauthv2.OAuthV2;


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
	protected OAuthV2 oauth;
	
	public TencentSenseParser(Configuration conf) {
		super(conf);
	}
	
	public TencentSenseParser(Configuration conf,OAuthV2 v2){
		super(conf);
		this.oauth = v2;
	}

	public static TencentSenseParser getTencentSenseFrom(Configuration conf ,SenseCrawlItem item){
		String ClientId = conf.get("ClientId");
		String OpenId = conf.get("OpenId");
		String AccessToken = conf.get("AccessToken");
//		String AccessTokenSecret = conf.get("AccessTokenSecret");
		OAuthV2 oauth = new OAuthV2();
//		OAuthV1 oauth = new OAuthV1();

		oauth.setClientId(ClientId);
		oauth.setOpenid(OpenId);
		oauth.setOauthVersion(OAuthConstants.OAUTH_VERSION_2_A);
		oauth.setAccessToken(AccessToken);

		String clusterName = conf.get("hbase.cluster.name");
		String namespace = conf.get("hbase.namespace");
		client = HBaseClientFactory.getClientWithCustomSeri(clusterName, namespace);
		if(item.containParam(CrawlItemEnum.KEYWORD.getName())){
			return new TencentSenseParserWord(conf,oauth);
		}else if(item.containParam(CrawlItemEnum.ACCOUNT.getName())){
			return new TencentSenseParserUser(conf,oauth);
		}else{
			throw new RuntimeException("wrong word type input!");
		}

	}
	
	protected Map<String, Object> fetchUserInfo(String name,UserAPI userAPI,OAuthV2 oauth,TencentJsonParser parser) {
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
