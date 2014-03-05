package com.sdata.live;

import org.apache.hadoop.hbase.util.Bytes;

import com.lakeside.core.utils.StringUtils;
import com.sdata.proxy.item.SenseCrawlItem;

/**
 * @author zhufb
 *
 */
public class IDBuilder {

	// with object id and origin id
	public static byte[] build(SenseCrawlItem item,String id){
		return build(((LiveItem)item).getObjectId(),item.getSourceName(),id);
	}
	
	/**
	 * 规则：btyes(objectID[Long,8位])+btyes(source.hashcode[int,4位])+btyes(id.hashcode[int,4位])
	 * @param objectId
	 * @param source
	 * @param id
	 * 
	 * @return
	 */
	public static byte[] build(Long objectId,String source,String id){
		byte[] boid = objectId == null?new byte[0]:Bytes.toBytes(objectId);
		byte[] bs = Bytes.toBytes(source.hashCode());
		byte[] bid = Bytes.toBytes(id.hashCode());
		return Bytes.add(boid, bs, bid);
		
	}
	
	/**
	 * 规则：btyes(objectID[Long,8位])+btyes(source.hashcode[int,4位])+btyes(id.hashcode[int,4位])
	 * @param objectId
	 * @param source
	 * @param id
	 * 
	 * @return
	 */
	public static byte[] build(Object objectId,Object source,Object id){
		Long oid = objectId == null?null:Long.valueOf(StringUtils.valueOf(objectId));
		String strSource = StringUtils.valueOf(source);
		String strid = StringUtils.valueOf(id);
		return build(oid,strSource,strid);
	}
}
