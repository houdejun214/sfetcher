package com.sdata.live.fetcher.tencent;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.jsoup.nodes.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.framework.db.hbase.thrift.HBaseClient;
import com.framework.db.hbase.thrift.HBaseClientFactory;
import com.lakeside.core.utils.StringUtils;
import com.lakeside.download.http.HttpPageLoader;
import com.sdata.context.config.Configuration;
import com.sdata.context.config.Constants;
import com.sdata.core.RawContent;
import com.sdata.core.item.CrawlItemEnum;
import com.sdata.core.parser.ParseResult;
import com.sdata.live.fetcher.LiveBaseWithTime;
import com.sdata.proxy.item.SenseCrawlItem;
import com.sdata.proxy.resource.Resources;
import com.tencent.weibo.api.UserAPI;
import com.tencent.weibo.beans.OAuth;
import com.tencent.weibo.oauthv1.OAuthV1;


/**
 * @author zhufb
 *
 */
public abstract class LiveTencentBase implements LiveBaseWithTime{
	
	protected static final Logger log = LoggerFactory.getLogger("Live.LiveTencentFetcher");
	protected String format = "json";
	protected static  HBaseClient client;
	protected static Map<String,String> header = new HashMap<String,String>();
	protected static HttpPageLoader pageLoader = HttpPageLoader.getAdvancePageLoader();
	protected static OAuth oauth;
	
	public static LiveTencentBase getFetcher(Configuration conf,SenseCrawlItem item){
		init(conf);
		if(item.containParam(CrawlItemEnum.KEYWORD.getName())){
			return new LiveTencentFromWord(conf);
		}else if(item.containParam(CrawlItemEnum.ACCOUNT.getName())){
			return new LiveTencentFromUser(conf);
		}else{
			throw new RuntimeException("wrong word type input!");
		}
	}
	
	private static void init(Configuration conf){
		if(client == null){
			Lock lock = new ReentrantLock();
			lock.lock();
			if(client == null){
				//Oauth 1
				OAuthV1 oauth1 = new OAuthV1();
				String consumerKey = conf.get("ConsumerKey");
				String consumerSecret = conf.get("ConsumerSecret");
				String AccessToken = conf.get("AccessToken");
				String AccessTokenSecret = conf.get("AccessTokenSecret");
				oauth1.setOauthConsumerKey(consumerKey);
				oauth1.setOauthConsumerSecret(consumerSecret);
				oauth1.setOauthToken(AccessToken);
				oauth1.setOauthTokenSecret(AccessTokenSecret);
				oauth = oauth1;
				String clusterName = conf.get("hbase.cluster.name");
				String namespace = conf.get("hbase.namespace");
				client = HBaseClientFactory.getClientWithCustomSeri(clusterName, namespace);
			}
			lock.unlock();
		}
	}
	
	public boolean isValid(String html){
		if(StringUtils.isEmpty(html)){
			return false;
		}
		if(html.indexOf("搜太多啦，服务器累得回火星了") >= 0){
			return false;
		}
		if(html.indexOf("login_div")>=0){
			return false;
		}
		return true;
	}
	
	public void refreshResource(){
		header.put("Cookie",Resources.Tencent.get().getCookie());
	}
	
	protected Map<String, Object> fetchUserInfo(String name,UserAPI userAPI,OAuth oauth,LiveTencentParser parser) {
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
}
