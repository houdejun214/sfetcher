package com.sdata.live.fetcher.tencent;

import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.jsoup.nodes.Document;
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
import com.sdata.core.resource.ResourceFactory;
import com.sdata.live.DBFactory;
import com.sdata.proxy.SenseFetchDatum;
import com.sdata.proxy.item.SenseCrawlItem;
import com.sdata.proxy.parser.SenseParser;
import com.tencent.weibo.api.UserAPI;
import com.tencent.weibo.beans.OAuth;
import com.tencent.weibo.oauthv1.OAuthV1;


/**
 * @author zhufb
 *
 */
public abstract class TencentSenseParser extends SenseParser{
	
	protected static final Logger log = LoggerFactory.getLogger("SdataCrawler.TencentSenseFrom");
	protected static final String Resource_source = "tencent";
	protected String format = "json";
	protected static  HBaseClient client;
	protected OAuth oauth;
	protected Map<String,String> header = new HashMap<String,String>();
	protected static HttpPageLoader pageLoader = HttpPageLoader.getAdvancePageLoader();
	protected static ResourceFactory<TencentResource> resourceFactory; 
	 
	static {
		resourceFactory = new ResourceFactory<TencentResource>(Resource_source,DBFactory.getResourceDB(),TencentResource.class);
	}
	
	public TencentSenseParser(Configuration conf) {
		super(conf);
	}
	
	public TencentSenseParser(Configuration conf,OAuth v){
		super(conf);
		this.oauth = v;
		refreshHeader();
	}

	public static TencentSenseParser getTencentSenseFrom(Configuration conf ,SenseCrawlItem item){
		
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
			return new TencentSenseParserWord(conf,oauth);
		}else if(item.containParam(CrawlItemEnum.ACCOUNT.getName())){
			return new TencentSenseParserUser(conf,oauth);
		}else{
			throw new RuntimeException("wrong word type input!");
		}
	}
	

	protected boolean isValid(Document document){
		return document !=null&&document.toString().indexOf("搜太多啦，服务器累得回火星了") < 0&&document.toString().indexOf("腾讯微博_你的心声")<0;
	}
	
	public void refreshHeader(){
		header.put("Cookie",resourceFactory.getResource().getCookie());
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
	

	protected static void await(long millis){
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
