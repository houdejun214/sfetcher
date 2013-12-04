package com.sdata.future;

import java.util.Map;

import com.lakeside.core.utils.MapUtils;
import com.lakeside.core.utils.StringUtils;
import com.sdata.sense.item.SenseCrawlItem;

/**
 * @author zhufb
 *
 */
public class FutureItem extends SenseCrawlItem {
	private String UID = "dtf_uid";
	private String tags;
	
	public FutureItem(Map<String, Object> map) {
		super(map);
		if(map == null){
			return;
		}
		this.tags = MapUtils.getString(map, "tags");
	}
	
	public String getTags() {
		return tags;
	}
	
	public Map<String,Object> toMap() {
		Map<String, Object> result = super.toMap();
		result.put("dtf_tags", tags);
		return result;
	}

	@Override
	public String parse() {
		StringBuffer sb = new StringBuffer();
		sb.append(tags).append(":").append(this.getParam(UID));
		return sb.toString();
	}
	
}
