package com.sdata.component.site;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.Iterator;
import java.util.List;

import com.lakeside.core.utils.ByteUtilities;
import com.lakeside.core.utils.StringUtils;
import com.sdata.core.RegionArea;
import com.sdata.core.site.BaseDataApi;
import com.sdata.core.site.PageQuery;

public class FlickrApi extends BaseDataApi {
	
	private static final String BoundBoxUrlPattern = "http://api.flickr.com/services/rest/?method=flickr.photos.search&api_key={0}&extras=date_upload,date_taken,geo,owner_name,url_l,url_o&bbox={1},{2},{3},{4}&page={5}&tags={6}&text={7}";

	private static final String TextSearchUrlPattern = "http://api.flickr.com/services/rest/?method=flickr.photos.search&api_key={0}&tags={1}&text={2}&sort=interestingness-desc&extras=date_upload,date_taken,geo,owner_name,url_l,url_o&page={3}&per_page={4}";
	
	private static final String GeoSearchUrlPattern = "http://api.flickr.com/services/rest/?method=flickr.photos.search&api_key={0}&lat={1}&lon={2}&radius=1&sort=interestingness-desc&extras=date_upload,date_taken,geo,owner_name,url_l,url_o&page={3}&per_page={4}";
	
	private static final String GetPhotoInfoUrlPattern = "http://api.flickr.com/services/rest/?method=flickr.photos.getInfo&api_key={0}&photo_id={1}";
	
	private static final String getUserContactsUrlPattern = "http://api.flickr.com/services/rest/?method=flickr.contacts.getPublicList&api_key={0}&user_id={1}&page={2}&per_page=1000&format=json&nojsoncallback=1";
	
	private static final String getUserInfoUrlPattern = "http://api.flickr.com/services/rest/?method=flickr.people.getInfo&api_key={0}&user_id={1}&format=json&nojsoncallback=1";
	
	private static final String getGroupInfoUrlPattern = "http://api.flickr.com/services/rest/?method=flickr.groups.getInfo&api_key={0}&group_id={1}&format=json&nojsoncallback=1";
	
	private static final String getUserPhotoesUrlPattern = "http://api.flickr.com/services/rest/?method=flickr.people.getPublicPhotos&api_key={0}&user_id={1}&page={2}&per_page=200&format=json&extras=date_upload,date_taken,geo,owner_name,url_l,url_o&nojsoncallback=1";
	
	private static final String getUserGroupsUrlPattern = "http://api.flickr.com/services/rest/?method=flickr.people.getPublicGroups&api_key={0}&user_id={1}&format=json&nojsoncallback=1";
	
	private static final String ApiKey="072fc0ba7c94462cbfac6a9c8f366106";
	
	public static final int PerPage = 500;
	
	public FlickrApi(){
		this.setApiKey("072fc0ba7c94462cbfac6a9c8f366106");
	}
	
	public String getSearchQueryUrl(String query,RegionArea area, int page){
		if(page<=0){
			page=1;
		}
		String queryUrl=MessageFormat.format(BoundBoxUrlPattern, 
				ApiKey,
				area.getMinX().toString(),
				area.getMinY().toString(),
				area.getMaxX().toString(),
				area.getMaxY().toString(),
				StringUtils.valueOf(page),query,query);
		return repairLink(queryUrl);
	}
	
	public String getSearchQueryUrl(PageQuery query) {
		int page = query.getPage();
		if(page<=0){
			page=1;
		}
		String keywords = encodeQueryWords(query.getQuery());
		String queryUrl=MessageFormat.format(TextSearchUrlPattern,ApiKey,keywords,keywords,StringUtils.valueOf(page),PerPage);
		if(query.getMinTime()!=null ){
			queryUrl+="&min_upload_date="+dateToString(query.getMinTime());
		}
		if(query.getMaxTime()!=null){
			queryUrl+="&max_upload_date="+dateToString(query.getMaxTime());
		}
		return repairLink(queryUrl);
	}
	
	/**
	 * Url for get flickr UserContacts 
	 * @param query
	 * @return
	 * @author qiumm
	 */
	public String getUserContactsUrl(String query,int page){
		if(page<=0){
			page=1;
		}
		String userId = query;
		String queryUrl=MessageFormat.format(getUserContactsUrlPattern,ApiKey,userId,StringUtils.valueOf(page));
		return repairLink(queryUrl);
		
	}
	
	/**
	 * Url for get flickr User's public photoes
	 * @param query
	 * @return
	 * @author qiumm
	 */
	public String getUserGroupsUrl(String query){
		String userId = query;
		String queryUrl=MessageFormat.format(getUserGroupsUrlPattern,ApiKey,userId);
		return repairLink(queryUrl);
		
	}
	
	/**
	 * Url for get photo's favorites list
	 * @param query
	 * @return
	 * @author qiumm
	 */
	public String getPhotosFavoritesUrl(String photo_id,int page){
		String baseUrl = "http://api.flickr.com/services/rest/";
		List<Parameter> parameters = new ArrayList<Parameter>();
		parameters.add(new Parameter("method", "flickr.photos.getFavorites"));
		parameters.add(new Parameter("api_key",	"07bf252557848f2535ac021debddd8fa"));
		parameters.add(new Parameter("photo_id", photo_id));
		parameters.add(new Parameter("page", StringUtils.valueOf(page)));
		parameters.add(new Parameter("per_page", 50));
		parameters.add(new Parameter("format", "json"));
		parameters.add(new Parameter("nojsoncallback", "1"));
		parameters.add(new Parameter("auth_token","72157629911612395-b953593c79a01db2"));	
		String signature = getSignature("c16514bbbcee58b6", parameters);
		parameters.add(new Parameter("api_sig",	signature));
		String url = getUrl(baseUrl, parameters);
		return repairLink(url);
		
	}
	
	/**
	 * Url for get group's members list
	 * @param query
	 * @return
	 * @author qiumm
	 */
	public String getGroupMembersUrl(String group_id, int page) {
		String baseUrl = "http://api.flickr.com/services/rest/";
		List<Parameter> parameters = new ArrayList<Parameter>();

		parameters.add(new Parameter("method", "flickr.groups.members.getList"));
		parameters.add(new Parameter("api_key",	"07bf252557848f2535ac021debddd8fa"));
		parameters.add(new Parameter("group_id", group_id));
		parameters.add(new Parameter("page", StringUtils.valueOf(page)));
		parameters.add(new Parameter("per_page", 500));
		parameters.add(new Parameter("format", "json"));
		parameters.add(new Parameter("nojsoncallback", "1"));
		parameters.add(new Parameter("auth_token","72157629911612395-b953593c79a01db2"));	
		String signature = getSignature("c16514bbbcee58b6", parameters);
		parameters.add(new Parameter("api_sig",	signature));
		String url = getUrl(baseUrl, parameters);
		return repairLink(url);

	}
	
	private static String getUrl(String baseUrl,List<Parameter> parameters){
		StringBuilder url = new StringBuilder(baseUrl);
		for(Parameter p:parameters){
			if(url.indexOf("?")<0){
				url.append("?");
			}else{
				url.append("&");
			}
			url.append(p.getName()+"="+p.getValue());
		}
		return url.toString();
	}
	
	/**
	 * Url for get group's images list
	 * @param query
	 * @return
	 * @author qiumm
	 */
	public String getGroupImagesUrl(String group_id,int page){
		String baseUrl = "http://api.flickr.com/services/rest/";
		List<Parameter> parameters = new ArrayList<Parameter>();
		parameters.add(new Parameter("method", "flickr.groups.pools.getPhotos"));
		parameters.add(new Parameter("api_key",	"07bf252557848f2535ac021debddd8fa"));
		parameters.add(new Parameter("group_id", group_id));
		parameters.add(new Parameter("page", StringUtils.valueOf(page)));
		parameters.add(new Parameter("per_page", 500));
		parameters.add(new Parameter("format", "json"));
		parameters.add(new Parameter("nojsoncallback", "1"));
		parameters.add(new Parameter("auth_token","72157629911612395-b953593c79a01db2"));	
		String signature = getSignature("c16514bbbcee58b6", parameters);
		parameters.add(new Parameter("api_sig",	signature));
		String url = getUrl(baseUrl, parameters);
		return repairLink(url);
		
	}
	
	/**
	 * Url for get flickr User's public photoes
	 * @param query
	 * @return
	 * @author qiumm
	 */
	public String getUserPhotoesUrl(String query,int page){
		if(page<=0){
			page=1;
		}
		String userId = query;
		String pageStr = StringUtils.valueOf(page);
		String queryUrl=MessageFormat.format(getUserPhotoesUrlPattern,ApiKey,userId,pageStr);
		return repairLink(queryUrl);
	}
	
	/**
	 * Url for get flickr UserInfo 
	 * @param query
	 * @return
	 * @author qiumm
	 */
	public String getUserInfoUrl(String query){
		String userId = query;
		String queryUrl=MessageFormat.format(getUserInfoUrlPattern,ApiKey,userId);
		return repairLink(queryUrl);
	}
	
	/**
	 * Url for get flickr GroupInfo 
	 * @param query
	 * @return
	 * @author qiumm
	 */
	public String getGroupInfoUrl(String query){
		String groupId = query;
		String queryUrl=MessageFormat.format(getGroupInfoUrlPattern,ApiKey,groupId);
		return repairLink(queryUrl);
		
	}
	
	public String dateToString(Date time){
		return String.valueOf(time.getTime()/1000);
		//return DateTimeUtils.format(time, "yyyy-MM-dd HH:mm:ss");
	}
	
	private String encodeQueryWords(String query){
		String[] splits = query.split("\\+");
		if(splits!=null && splits.length>0){
			StringBuilder sb = new StringBuilder();
			for(String s:splits){
				try {
					if(s==null){
						s="";
					}
					if(sb.length()>0){
						sb.append("+");
					}
					String encode = URLEncoder.encode(s, "UTF-8");
					sb.append(encode.replaceAll("\\+", "%20"));
				} catch (UnsupportedEncodingException e) {
					e.printStackTrace();
				}
			}
			return sb.toString();
		}
		return query;
	}
	
	public static String getSignature(String sharedSecret, List<Parameter> params) {
		StringBuffer buffer = new StringBuffer();
		buffer.append(sharedSecret);
		Collections.sort(params, new Comparator<Parameter>(){
			public int compare(Parameter o1, Parameter o2) {
				return o1.getName().compareTo(o2.getName());
			}
		});
		Iterator iter = params.iterator();
		while (iter.hasNext()) {
			Parameter param = (Parameter) iter.next();
			buffer.append(param.getName());
			buffer.append(param.getValue());
		}
		try {
			MessageDigest md = MessageDigest.getInstance("MD5");
			return ByteUtilities.toHexString(md.digest(buffer.toString()
					.getBytes("UTF-8")));
		} catch (NoSuchAlgorithmException e) {
			throw new RuntimeException(e);
		} catch (UnsupportedEncodingException u) {
			throw new RuntimeException(u);
		}
	}
	
	static class Parameter{
		private String name;
		private String value;

		public String getName() {
			return name;
		}

		public void setName(String name) {
			this.name = name;
		}

		public String getValue() {
			return value;
		}

		public void setValue(String value) {
			this.value = value;
		}
		
		Parameter(String name,String value){
			this.name=name;
			this.value = value;
		}
		Parameter(String name,int value){
			this.name=name;
			this.value = String.valueOf(value);
		}
	}
}

