package com.sdata.component.fetcher;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.lakeside.core.utils.StringUtils;
import com.sdata.component.site.FlickrApi;
import com.sdata.core.Constants;
import com.sdata.core.NegligibleException;
import com.sdata.core.RawContent;
import com.sdata.core.fetcher.SdataFetcher;
import com.sdata.core.parser.ParseResult;
import com.sdata.core.parser.SdataParser;
import com.sdata.core.util.WebPageDownloader;

/**
 * fetch user relationship of Tencent weibo
 * 
 * the fetcher use multiple-threads to fetch list of a user' relationship 
 * 
 * @author houdj
 *
 */
public abstract class FlickrBaseFetcher extends SdataFetcher{
	
	private static final Logger log = LoggerFactory.getLogger("SdataCrawler.FlickrBaseFetcher");
	
	private final FlickrApi flickrAPI = new FlickrApi();

	/**
	 * fetch user's  infomation
	 * @param id
	 * @return
	 * @author qiumm
	 */
	protected Map<String,Object> fetchUserMeta(String id,SdataParser userparser){
		String userJson = null;
		try {
			String queryUrl = flickrAPI.getUserInfoUrl(id);
//			log.info("fetching user["+id+"] user info ,url is: "+queryUrl);
			userJson = WebPageDownloader.download(queryUrl);
		} catch (Exception e) {
			e.printStackTrace();
		}
		RawContent rc = new RawContent(userJson);
		rc.setMetadata("type", Constants.PARSER_TYPE_USER);
		ParseResult parseResult = userparser.parseSingle(rc);
		Map<?, ?> userMap = parseResult.getMetadata();
		Map<String, Object> userInfoMap = (Map<String, Object>)userMap.get(Constants.USER);
		String id_s = id.replace("@", "0").replace("N", "1");
		Long _id = Long.valueOf(id_s);
		userInfoMap.put(Constants.OBJECT_ID, _id);
		userInfoMap.put(Constants.FLICKR_GROUPLIST, this.fetchUserGroups(id,userparser));
		String stat = StringUtils.valueOf(userInfoMap.get("stat"));
		String message = StringUtils.valueOf(userInfoMap.get("message"));
		if(!stat.equals("ok")){
			log.warn("fetch user["+id+"]'s profile stat ["+stat+"]:"+message);
		}
		Map<String, Object> map = ((Map<String, Object>) userMap.get(Constants.USER));
		return map;
		
	}
	
	/**
	 * fetch user's  group
	 * @param id
	 * @return
	 * @author geyong 
	 */
	private List<Map<String,Object>> fetchUserGroups(String id,SdataParser userparser){
		List<Map<String,Object>> resultList = new ArrayList<Map<String,Object>>();
		if(id.equals("")){
			throw new RuntimeException("user id is empty");
		}
		String content = null;
		try {
			String queryUrl = flickrAPI.getUserGroupsUrl(id);
//			log.info("fetching user["+id+"] group list ,url is: "+queryUrl);
			content = WebPageDownloader.download(queryUrl);
		} catch (Exception e) {
			e.printStackTrace();
		}
		RawContent rawContent = new RawContent(content);
		rawContent.setMetadata("type", Constants.PARSER_TYPE_USER_GROUP);
		ParseResult result = userparser.parseSingle(rawContent);
		if(result == null){
			throw new NegligibleException("fetch content is empty");
		}
		Map<?, ?> metadata = result.getMetadata();
		String stat = StringUtils.valueOf(metadata.get("stat"));
		String message = StringUtils.valueOf(metadata.get("message"));
		if(!stat.equals("ok")){
			log.warn("fetch user["+id+"]'s group list stat ["+stat+"]:"+message);
			return resultList;
		}
		String totalPages = StringUtils.valueOf(metadata.get("pages"));
		if(totalPages.equals("0")){
			log.warn("user["+id+"] do't has any group " );
			return resultList;
		}
		List<Map<String,Object>> groupList = (List<Map<String,Object>>)metadata.get(Constants.FLICKR_GROUPLIST);
		resultList.addAll(groupList);
//		log.info("fetch user["+id+"]'s group list count:"+resultList.size());
		return resultList;
		
	}
	
}
