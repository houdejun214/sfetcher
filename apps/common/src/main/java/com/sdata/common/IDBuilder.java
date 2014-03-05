package com.sdata.common;

import org.apache.hadoop.hbase.util.Bytes;

import com.lakeside.core.utils.StringUtils;

/**
 * @author zhufb
 *
 */
public class IDBuilder {

	// with object id and origin id
	public static byte[] build(CommonItem item,int id){
		return build(item.getSourceName(),id);
	}
	
	/**
	 * 规则：btyes(source.hashcode[int,4位])+btyes([int,4位]) -- id is url.hashcode
	 * @param source
	 * @param id
	 * 
	 * @return
	 */
	public static byte[] build(String source,int id){
		byte[] bs = Bytes.toBytes(source.hashCode());
		byte[] bid = Bytes.toBytes(id);
		return Bytes.add(bs, bid);
		
	}
}
