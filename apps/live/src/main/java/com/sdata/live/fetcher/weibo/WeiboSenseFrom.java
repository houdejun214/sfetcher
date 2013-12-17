package com.sdata.live.fetcher.weibo;

import java.util.List;
import java.util.Map;

import weibo4j.WeiboHelper;

import com.lakeside.core.utils.StringUtils;
import com.sdata.core.FetchDatum;
import com.sdata.proxy.SenseFetchDatum;
import com.sdata.proxy.item.SenseCrawlItem;

/**
 * @author zhufb
 *
 */
public abstract class WeiboSenseFrom {
	private String host = "http://www.weibo.com/";
	
	public abstract List<FetchDatum> getData(SenseCrawlItem item);
	
	public abstract SenseFetchDatum getDatum(SenseFetchDatum datum);

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
