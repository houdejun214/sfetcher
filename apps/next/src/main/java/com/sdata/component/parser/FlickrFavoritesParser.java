package com.sdata.component.parser;


import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import net.sf.json.JSONArray;
import net.sf.json.JSONObject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.lakeside.core.utils.StringUtils;
import com.sdata.core.Configuration;
import com.sdata.core.Constants;
import com.sdata.core.RawContent;
import com.sdata.core.RunState;
import com.sdata.core.parser.ParseResult;
import com.sdata.core.parser.SdataParser;

public class FlickrFavoritesParser extends SdataParser{

	private static final Logger log = LoggerFactory.getLogger("SdataCrawler.FlickrFavoritesParser");
	
	@Override
	public ParseResult parseList(RawContent c) {
		return null;
	}

	@Override
	public ParseResult parseSingle(RawContent c) {
		ParseResult result = new ParseResult();
		Map<String, Object> metadata = new HashMap<String, Object>();
		if (c.isEmpty()) {
			log.warn("fetch content is empty!");
			return null;
		}
		String content = c.getContent();
		JSONObject JSObj = JSONObject.fromObject(content);
		String stat = StringUtils.valueOf(JSObj.get("stat"));
		String message = StringUtils.valueOf(JSObj.get("message"));
		metadata.put("stat", stat);
		metadata.put("message", message);
		JSONObject photoinfo = (JSONObject) JSObj.get("photo");
		if (photoinfo != null) {
			List<Map<String,Object>> favoritesList = new ArrayList<Map<String,Object>>();
			JSONArray favoritesArray = (JSONArray)photoinfo.get("person");
			if(favoritesArray!=null){
				Iterator<JSONObject> favoritesIterator= favoritesArray.iterator();
				while(favoritesIterator.hasNext()){
					JSONObject  favoritesObj = favoritesIterator.next();
					Map<String,Object> favoritesMap = new HashMap<String,Object>();
					String nsid = StringUtils.valueOf(favoritesObj.get("nsid"));
					if(nsid!=null && StringUtils.isNotEmpty(nsid.toString())){
						String uid = nsid.replace("@", "0").replace("N", "1");
						favoritesMap.put("uid", Long.parseLong(uid));
						favoritesMap.put("nsid", nsid);
					}
					String username = StringUtils.valueOf(favoritesObj.get("username"));
					favoritesMap.put("username", username);
					String favedate = StringUtils.valueOf(favoritesObj.get("favedate"));
					favoritesMap.put("favedate", favedate);
					String iconserver = StringUtils.valueOf(favoritesObj.get("iconserver"));
					favoritesMap.put("iconserver", iconserver);
					String iconfarm = StringUtils.valueOf(favoritesObj.get("iconfarm"));
					favoritesMap.put("iconfarm", iconfarm);
					favoritesList.add(favoritesMap);
				}
			}
			String page = StringUtils.valueOf(photoinfo.get("page"));
			String pages = StringUtils.valueOf(photoinfo.get("pages"));
			metadata.put("page", page);
			metadata.put("pages", pages);
			metadata.put(Constants.FLICKR_FAVORITES, favoritesList);
			metadata.put("totalNum", favoritesList.size());
		} 
		result.setMetadata(metadata);
		return result;
	}
	
	public FlickrFavoritesParser(Configuration conf,RunState state){
		setConf(conf);
	}
	
}
