package com.sdata.live;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import net.sf.json.JSONObject;

import com.lakeside.core.utils.JSONUtils;
import com.lakeside.core.utils.MapUtils;
import com.lakeside.core.utils.StringUtils;
import com.lakeside.core.utils.UrlFormater;
import com.lakeside.core.utils.UrlUtils;
import com.sdata.sense.Constants;
import com.sdata.sense.item.SenseCrawlItem;

/**
 * 
 * Crawl item for crawler from SCMS
 * @author zhufb
 *
 */
public class LiveItem extends SenseCrawlItem {
	
	public LiveItem(Map<String,Object> map){
		super(map);
		if(map == null){
			return;
		}
		this.fields = MapUtils.getString(map,"fields");
		this.objectId = MapUtils.getLong(map, "object_id");
		this.areas = MapUtils.getString(map,"areas");
		
		//Test end
		this.init();
	}
	
	protected String  fields;
	protected Long objectId;	
	protected String  areas;
	protected Map<String,Object> fieldMap = new HashMap<String, Object>();
	
	public Map<String, Object> getFields() {
		return fieldMap;
	}
	
	protected void init() {
		if(JSONUtils.isValidJSON(fields)){
			fieldMap = JSONObject.fromObject(fields);
		}
	}

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
	
	public Long getObjectId() {
		return objectId;
	}

	public String getAreas() {
		return areas;
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
		return LiveItem.class.getName();
	}
	
	@Override
	public Map<String,Object> toMap() {
		Map<String,Object> result = super.toMap();
		result.put(Constants.DATA_TAGS_FROM_OBJECT_ID,this.getObjectId());
		result.putAll(this.getFields());
		return result;
	}
}
