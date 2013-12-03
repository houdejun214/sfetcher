package com.sdata.live;

import org.apache.hadoop.hbase.util.Bytes;

import com.lakeside.core.utils.StringUtils;
import com.sdata.sense.item.SenseCrawlItem;

/**
 * @author zhufb
 *
 */
public class IDBuilder {

	// with object id and origin id
	public static byte[] build(SenseCrawlItem item,String id){
		return build(item.getSourceName(),((LiveItem)item).getObjectId(),id);
	}
	
	/**
	 * 规则：btyes(objectID[Long,8位])+btyes(source.hashcode[int,4位])+btyes(id.hashcode[int,4位])
	 * 
	 * @param source
	 * @param objectId
	 * @param id
	 * @return
	 */
	public static byte[] build(String source,Long objectId,String id){
		byte[] boid = Bytes.toBytes(objectId);
		byte[] bs = Bytes.toBytes(source.hashCode());
		byte[] bid = Bytes.toBytes(id.hashCode());
		return  Bytes.add(boid, bs, bid);
		
	}
	
	/**
	 * 规则：btyes(objectID[Long,8位])+btyes(source.hashcode[int,4位])+btyes(id.hashcode[int,4位])
	 * 
	 * @param source
	 * @param objectId
	 * @param id
	 * @return
	 */
	public static byte[] build(Object source,Object objectId,Object id){
		String strSource = StringUtils.valueOf(source);
		Long oid = Long.valueOf(StringUtils.valueOf(objectId));
		String strid = StringUtils.valueOf(id);
		return build(strSource,oid,strid);
	}
}
