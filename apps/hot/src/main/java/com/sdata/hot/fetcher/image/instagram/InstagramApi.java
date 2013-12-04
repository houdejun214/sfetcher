package com.sdata.hot.fetcher.image.instagram;

import java.text.MessageFormat;

import net.sf.json.JSONObject;

import com.lakeside.download.http.HttpPageLoader;
import com.sdata.context.config.Configuration;
import com.sdata.core.site.BaseDataApi;

/**
 * @author zhufb
 *
 */
public class InstagramApi extends BaseDataApi {
	
	private static final String getPopularSearchUrlPattern = "https://api.instagram.com/v1/media/popular?access_token={0}";
	
	private String access_token = "202609190.933bfcd.afacf75c6db14c0bab6b1fd034007ba2";
	
	public InstagramApi(Configuration conf){
		this.access_token = conf.get("AccessToken", access_token);
	}
	
	public String getQueryUrl() {
		String queryUrl= MessageFormat.format(getPopularSearchUrlPattern, access_token);
		return repairLink(queryUrl);
	}
	
	public JSONObject getPopular() {
		String content = HttpPageLoader.getDefaultPageLoader().download(getQueryUrl()).getContentHtml();
		JSONObject json = JSONObject.fromObject(content);
		return json;
	}
	public String getAccess_token() {
		return access_token;
	}
}

