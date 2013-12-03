package com.sdata.future;

import org.apache.hadoop.hbase.util.Bytes;

import com.lakeside.core.utils.UUIDUtils;
import com.sdata.sense.item.SenseCrawlItem;

/**
 * @author zhufb
 *
 */
public class FutureIDBuilder {

	// with object id and origin id
	public static byte[] build(SenseCrawlItem item,Long id){
		return build(item.getSourceName(),((FutureItem)item).getTags(),id);
	}
	
	/**
	 * 
	 * 规则：btyes(sourec.hashcode[int,4位])+btyes(tag.hashcode[int,4位])+btyes(id[long,8位])
	 * @param source  int 
	 * @param tag
	 * @param id
	 * @return
	 */
	public static byte[] build(Object source,String tag,Long id){
		byte[] sb = Bytes.toBytes(source.hashCode());
		byte[] tb = Bytes.toBytes(tag.hashCode());
		byte[] idb = Bytes.toBytes(id);
		return  Bytes.add(sb, tb, idb);
	}
}
