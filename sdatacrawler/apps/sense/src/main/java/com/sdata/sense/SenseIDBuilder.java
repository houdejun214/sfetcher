package com.sdata.sense;

import com.lakeside.core.utils.UUIDUtils;
import com.sdata.sense.item.SenseCrawlItem;

/**
 * @author zhufb
 *
 */
public class SenseIDBuilder {

	// with object id and origin id
	public static String build(SenseCrawlItem item,String id){
		return build(item.getSourceName(),item.getObjectId(),id);
	}
	
	public static String build(Object source,Object objectid,Object id){
		StringBuilder sb = new StringBuilder();
		sb.append(id);
		sb.append("_");
		sb.append(source);
		sb.append("_");
		sb.append(objectid);
		return UUIDUtils.encode(UUIDUtils.getMd5UUID(sb.toString()));
	}
}
