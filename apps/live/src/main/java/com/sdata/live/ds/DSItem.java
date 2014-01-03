package com.sdata.live.ds;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import net.sf.json.JSONObject;

import com.lakeside.core.utils.JSONUtils;
import com.lakeside.core.utils.MapUtils;
import com.lakeside.core.utils.StringUtils;
import com.lakeside.core.utils.UrlFormater;
import com.lakeside.core.utils.UrlUtils;
import com.sdata.live.LiveItem;

/**
 * 
 * Crawl item for crawler from SCMS
 * 
 * @author zhufb
 *
 */
public class DSItem extends LiveItem {
	
	public DSItem(Map<String,Object> map){
		super(map);
		if(map == null){
			return;
		}
		this.fields = MapUtils.getString(map,"fields");
		if(JSONUtils.isValidJSON(fields)){
			fieldMap = JSONObject.fromObject(fields);
		}
	}
	
	protected String  fields;
	protected Map<String,Object> fieldMap = new HashMap<String, Object>();
	
	public String parse(String charset){
		if(StringUtils.isEmpty(charset)){
			charset = "UTF-8";
		}
		UrlFormater formater = new UrlFormater(this.entryUrl);
		Map<String,Object> params = new HashMap<String, Object>();
		for(Entry<String, Object> e:paramMap.entrySet()){
			params.put(e.getKey(), UrlUtils.encode(StringUtils.valueOf(e.getValue()),charset));
		}
		return formater.format(params);
	}

	public Map<String, Object> getFieldMap() {
		return fieldMap;
	}

	@Override
	public String parse(){
		return this.parse(null);
	}
	@Override
	public String toString(){
		return DSItem.class.getName();
	}
	
	@Override
	public Map<String,Object> toMap() {
		Map<String,Object> result = super.toMap();
		result.putAll(this.getFieldMap());
		return result;
	}
}
