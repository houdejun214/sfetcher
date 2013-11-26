package com.sdata.hot.fetcher.image.flickr;

import java.text.MessageFormat;

import net.sf.json.JSONObject;

import com.lakeside.core.utils.PatternUtils;
import com.lakeside.download.http.HttpPageLoader;
import com.sdata.core.Configuration;
import com.sdata.core.site.BaseDataApi;

/**
 * @author zhufb
 *
 */
public class FlickrApi extends BaseDataApi {
	
	private static final String woeid = "23424948";
	private static final String searchUrl = "http://api.flickr.com/services/rest/?method=flickr.photos.search&api_key={0}&min_upload_date={1}&sort=interestingness-desc&content_type=1&woe_id={2}&media=photos&extras=description%2C+license%2C+date_upload%2C+date_taken%2C+owner_name%2C+icon_server%2C+original_format%2C+last_update%2C+geo%2C+tags%2C+machine_tags%2C+o_dims%2C+views%2C+path_alias%2C+url_l%2C+url_z&per_page={3}&format=json";
	private static final String photoUrl = "http://api.flickr.com/services/rest/?method=flickr.photos.getInfo&api_key={0}&photo_id={1}&format=json";
	private String api_key = "ad683a522c6c2778fd8a3c93921b8a4f";
	private static final String REGEX ="(\\{.*\\})";
	public FlickrApi(Configuration conf){
		this.setApiKey(conf.get("api_key", api_key));
	}
	
	public JSONObject getPopular(Long minUploadTime,int count) {
		String queryUrl= MessageFormat.format(searchUrl, super.getApiKey(),String.valueOf(minUploadTime),woeid,count);
		queryUrl = repairLink(queryUrl);
		String content = HttpPageLoader.getDefaultPageLoader().download(queryUrl).getContentHtml();
		String matchPattern = PatternUtils.getMatchPattern(REGEX, content, 1);
		JSONObject json = JSONObject.fromObject(matchPattern);
		return json;
	}

	public JSONObject getPhoto(Long id) {
		String queryUrl= MessageFormat.format(photoUrl, super.getApiKey(),String.valueOf(id));
		queryUrl = repairLink(queryUrl);
		String content = HttpPageLoader.getDefaultPageLoader().download(queryUrl).getContentHtml();
		String matchPattern = PatternUtils.getMatchPattern(REGEX, content, 1);
		JSONObject json = JSONObject.fromObject(matchPattern);
		return json;
	}
}

