package com.sdata.live.fetcher.weibo;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.jsoup.nodes.Document;

import weibo4j.WeiboHelper;

import com.lakeside.core.utils.StringUtils;
import com.lakeside.download.http.HttpPageLoader;
import com.sdata.core.FetchDatum;
import com.sdata.proxy.SenseFetchDatum;
import com.sdata.proxy.item.SenseCrawlItem;
import com.sdata.proxy.resource.Resources;

/**
 * @author zhufb
 *
 */
public abstract class WeiboSenseFrom {
	protected String host = "http://www.weibo.com/";
	protected Map<String,String> httpHeader = new HashMap<String,String>();
	protected static HttpPageLoader advancePageLoader = HttpPageLoader.getAdvancePageLoader();
	

	public abstract List<FetchDatum> getData(SenseCrawlItem item);
	
	public abstract SenseFetchDatum getDatum(SenseFetchDatum datum);

	protected boolean isValid(String html){
		if(StringUtils.isEmpty(html)){
			return false;
		}
		if(html.contains("$CONFIG['islogin'] = '0'")){
			return false;
		}
		if(html.contains("你的行为有些异常，请输入验证码")){
			return false;
		}
		return true;
	}
	
	protected void refreshHeader(){
		httpHeader.put("Cookie",Resources.Weibo.get().getCookie());
	}
	
	protected String getTweetUrl(Map<String,Object> matadata){
		StringBuffer sb = new StringBuffer(host);
		Map user = (Map)matadata.get("user");
		if(user == null||user.size() == 0){
			return null;
		}
		Object id = matadata.get("id");
		if(id == null||!StringUtils.isNum(id.toString())){
			return null;
		}
		sb.append(user.get("id")).append("/").append(WeiboHelper.id2Mid((Long)id));
		return sb.toString();
	}
	
	protected void sleep(int s){
		try {
			Thread.sleep(s*1000);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
