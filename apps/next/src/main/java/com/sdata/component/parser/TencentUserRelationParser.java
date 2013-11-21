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

public class TencentUserRelationParser extends SdataParser{

	private static final Logger log = LoggerFactory.getLogger("SdataCrawler.TencentUserRelationParser");
	
	@Override
	public ParseResult parseList(RawContent c) {
		return null;
	}

	@Override
	public ParseResult parseSingle(RawContent c) {
		ParseResult result = new ParseResult();
		if(c.isEmpty()){
			log.warn("fetch content is empty!");
			return null;
		}
		JSONArray usersList = new JSONArray();
		String content = c.getContent();
		JSONObject JSObj = JSONObject.fromObject(content);
		if(JSObj==null){
			log.warn("fetch content is not valid, maybe there is exception when http response!");
			return null;
		}
		String errcode = StringUtils.valueOf(JSObj.get("errcode"));
		String msg = StringUtils.valueOf(JSObj.get("msg"));
		String ret = StringUtils.valueOf(JSObj.get("ret"));
		Map<String,Object> prMetadata = new HashMap<String,Object>();
		prMetadata.put("errcode", errcode);
		prMetadata.put("msg", msg);
		prMetadata.put("ret", ret);
		Object object = JSObj.get("data");
		if(object != null&&object instanceof JSONObject){
			JSONObject dataObj = (JSONObject)object;
			String hasnext = StringUtils.valueOf(dataObj.get("hasnext"));
			prMetadata.put("hasnext", hasnext);
			Object object2 = dataObj.get("info");
			if(object2!=null && object2 instanceof JSONArray){
				JSONArray infoArr = (JSONArray)object2;
				String id = StringUtils.valueOf(dataObj.get("id"));
				Iterator<JSONObject> infoIterator= infoArr.iterator();
				while(infoIterator.hasNext()){
					JSONObject infoObj = infoIterator.next();
					Object name = infoObj.get("name");
					if(name==null || StringUtils.isEmpty(name.toString())){
						//log.warn("user name is empty:"+infoObj.toString());
						continue;
					}
					usersList.add(infoObj);
				}
			}
		}
		prMetadata.put(Constants.USER, usersList);
		result.setMetadata(prMetadata);
		return result;
	}
	
	public TencentUserRelationParser(Configuration conf,RunState state){
		setConf(conf);
	}
	
	private long getIdCode(String uid){
		byte[] md5;
		try {
			md5 = StringUtils.md5(uid);
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
		// 
		long value = 0;
		for (int i = 0; i < md5.length; i++)
		{
		   value = (value << 8) + (md5[i] & 0xff);
		}
		return value;
	}

}


class TencentUserRelationParseResult extends ParseResult {
	
	private List<Map<String,Object>> newCategoryList = new ArrayList<Map<String,Object>>();

	public List<Map<String, Object>> getNewCategoryList() {
		return newCategoryList;
	}

	public void setNewCategoryList(List<Map<String, Object>> newCategoryList) {
		this.newCategoryList = newCategoryList;
	}
	
}