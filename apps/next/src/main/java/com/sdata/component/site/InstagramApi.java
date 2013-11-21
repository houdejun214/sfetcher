package com.sdata.component.site;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.text.MessageFormat;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.Iterator;
import java.util.List;

import com.lakeside.core.utils.ByteUtilities;
import com.lakeside.core.utils.StringUtils;
import com.sdata.core.Configuration;
import com.sdata.core.TimeSpanCircularArea;
import com.sdata.core.site.BaseDataApi;
import com.sdata.core.site.PageQuery;

public class InstagramApi extends BaseDataApi {
	
	private static final String getMediaInfoUrlPattern = "https://api.instagram.com/v1/media/{0}?access_token={1}";
	
	private static final String getMediaSearchUrlPattern = "https://api.instagram.com/v1/media/search?access_token={0}&lat={1}&lng={2}&distance={3}&min_timestamp={4}&max_timestamp={5}";
	
	private static final String getPopularSearchUrlPattern = "https://api.instagram.com/v1/media/popular?access_token={0}";
	
	private static final String getUserInforUrlPattern = "https://api.instagram.com/v1/users/{0}/?access_token={1}";
	
	private static final String getMediaCommentsUrlPattern = "https://api.instagram.com/v1/media/{0}/comments?access_token={1}";
	
	private static final String getMediaLikesUrlPattern = "https://api.instagram.com/v1/media/{0}/likes?access_token={1}";
	
	private static final String getTagSearchUrlPattern = "https://api.instagram.com/v1/tags/{0}/media/recent?access_token={1}";
	
	private static final String CRAWL_TYPE_MEDIA_SEARCH = "mediaSearch";
	
	private static final String CRAWL_TYPE_TAGS_SEARCH = "tagSearch";
	
	private static final String CRAWL_TYPE_MEDIA_POPULAR = "mediaPopular";
	
	private static final String ApiKey="933bfcda1334495b8dda61cc73b5a16f";
	
	private String access_token = "202609190.933bfcd.afacf75c6db14c0bab6b1fd034007ba2";
	
	private String crawlType;
	
	public InstagramApi(Configuration conf){
		this.setAccess_token(conf.get("AccessToken", "202609190.933bfcd.afacf75c6db14c0bab6b1fd034007ba2"));
		this.setCrawlType(conf.get("CrawlType", CRAWL_TYPE_MEDIA_SEARCH));
	}
	
	public String getQueryUrl(TimeSpanCircularArea area) {
		String queryUrl = null;
		if(crawlType.equals(CRAWL_TYPE_MEDIA_SEARCH)){
			queryUrl = getQueryUrlByMediaSearch(area);
		}else if(crawlType.equals(CRAWL_TYPE_MEDIA_POPULAR)){
			queryUrl = getQueryUrlByMediaPopular(area);
		}
		return queryUrl;
	}
	
	public String getUserInfoUrl(String uid) {
		String queryUrl= MessageFormat.format(getUserInforUrlPattern, uid,access_token);
		return repairLink(queryUrl);
	}
	
	public String getMediaCommentsUrl(String mediaId) {
		String queryUrl= MessageFormat.format(getMediaCommentsUrlPattern, mediaId,access_token);
		return repairLink(queryUrl);
	}
	
	public String getMediaLikesUrl(String mediaId) {
		String queryUrl= MessageFormat.format(getMediaLikesUrlPattern, mediaId,access_token);
		return repairLink(queryUrl);
	}
	
	public String getTagSearchUrl(String tag) {
		String queryUrl= MessageFormat.format(getTagSearchUrlPattern, tag,access_token);
		return repairLink(queryUrl);
	}
	
	public String getQueryUrlByMediaSearch(TimeSpanCircularArea area) {
		long min_timestamp = area.getStartTime().getTime()/1000;
		long max_timestamp = min_timestamp+ area.getTimeSpan();
		String queryUrl= MessageFormat.format(getMediaSearchUrlPattern, access_token,String.valueOf(area.getLoc().getLatitude()),String.valueOf(area.getLoc().getLongitude()),String.valueOf(area.getDistance()),String.valueOf(min_timestamp),String.valueOf(max_timestamp));
		return repairLink(queryUrl);
	}
	
	public String getQueryUrlByMediaPopular(TimeSpanCircularArea area) {
		String queryUrl= MessageFormat.format(getPopularSearchUrlPattern, access_token);
		return repairLink(queryUrl);
	}
	
	public String getSearchQueryUrl(PageQuery query) {
		int page = query.getPage();
		if(page<=0){
			page=1;
		}
		String keywords = encodeQueryWords(query.getQuery());
		String queryUrl=MessageFormat.format(getMediaSearchUrlPattern,ApiKey,keywords,keywords,StringUtils.valueOf(page));
		if(query.getMinTime()!=null ){
			queryUrl+="&min_upload_date="+dateToString(query.getMinTime());
		}
		if(query.getMaxTime()!=null){
			queryUrl+="&max_upload_date="+dateToString(query.getMaxTime());
		}
		return repairLink(queryUrl);
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
	 * Url for get media info 
	 * @param query
	 * @return
	 * @author geyong
	 */
	public String getMediaInfoUrl(String query){
		String mediaId = query;
		String queryUrl=MessageFormat.format(getMediaInfoUrlPattern,mediaId,ApiKey);
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

	public String getAccess_token() {
		return access_token;
	}

	public void setAccess_token(String access_token) {
		this.access_token = access_token;
	}

	public String getCrawlType() {
		return crawlType;
	}

	public void setCrawlType(String crawlType) {
		this.crawlType = crawlType;
	}
}

