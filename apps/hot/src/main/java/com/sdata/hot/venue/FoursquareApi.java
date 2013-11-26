package com.sdata.hot.venue;

import java.util.List;

import net.sf.json.JSONArray;
import net.sf.json.JSONObject;

import com.lakeside.core.utils.MapUtils;
import com.lakeside.download.http.HttpPageLoader;
import com.sdata.core.Configuration;
import com.sdata.core.site.BaseDataApi;

/**
 * @author zhufb
 *
 */
public class FoursquareApi extends BaseDataApi {

	private String access_token = "XOLJSOIZICC3YXLKWY2GXG5YIDAE3A5VCFQWRUOCJYCPWSDQ";
	private String treedsUrl = "https://api.foursquare.com/v2/venues/trending?ll=1.36,103.82&limit=%s&radius=40000&oauth_token=%s";
	private String venueUrl = "https://api.foursquare.com/v2/venues/%s?oauth_token=%s";
	private String exploreUrl = "https://api.foursquare.com/v2/venues/explore?near=singapore&section=%s&offset=%s&limit=%s&oauth_token=%s";
	public FoursquareApi(Configuration conf){
		this.access_token = conf.get("AccessToken", access_token);
	}
	
	public JSONArray getTrendingVenues(int count) {
		String queryUrl= String.format(treedsUrl,String.valueOf(count),access_token);
		String content = HttpPageLoader.getDefaultPageLoader().download(queryUrl).getContentHtml();
		JSONObject json = JSONObject.fromObject(content);
		if(json.isNullObject()){
			return null;
		}
		return json.getJSONObject("response").getJSONArray("venues");
	}

	public List getExploreVenues(String section,int offset,int limit) {
		String queryUrl= String.format(exploreUrl,section,String.valueOf(offset),String.valueOf(limit),access_token);
		String content = HttpPageLoader.getDefaultPageLoader().download(queryUrl).getContentHtml();
		JSONObject json = JSONObject.fromObject(content);
		Object venues = MapUtils.getInter(json, "response/groups/items/venue");
		if(venues == null||!(venues instanceof List)){
			return null;
		}
		return (List)venues;
	}
	
	public JSONObject getVenue(String id) {
		String queryUrl= String.format(venueUrl,id,access_token);
		String content = HttpPageLoader.getDefaultPageLoader().download(queryUrl).getContentHtml();
		JSONObject json = JSONObject.fromObject(content);
		return json.getJSONObject("response").getJSONObject("venue");
	}
	
	public String getAccess_token() {
		return access_token;
	}
}

