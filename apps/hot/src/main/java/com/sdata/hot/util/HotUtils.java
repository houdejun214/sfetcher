package com.sdata.hot.util;

import java.util.Date;
import java.util.Map;

import org.apache.hadoop.hbase.util.Bytes;

import com.lakeside.core.utils.StringUtils;
import com.lakeside.download.http.HttpPageLoader;
import com.sdata.core.RawContent;

/**
 * @author zhufb
 *
 */
public class HotUtils {

	private static int longSize = 8;
	private static int intSize = 4;
	
	public static void notify(String url){
		 HttpPageLoader.getDefaultPageLoader().download(url);
	}
	
	public static RawContent getRawContent(String url){
		return getRawContent(url,null);
	}
	
	public static RawContent getRawContent(String url,Map<String, String> header){
		if(StringUtils.isEmpty(url)){
			return null;
		}
		String content = HttpPageLoader.getDefaultPageLoader().download(header,url).getContentHtml();
		if(content == null){
			return null;
		}
		return new RawContent(url,content);
	}
	
	public static byte[] getRowkey(int type,Date fetTime,int rank){
		byte[] rowkey = new byte[longSize +intSize*2];
		byte[] tpbt = Bytes.toBytes(type);
		byte[] fetmbt = Bytes.toBytes(Long.MAX_VALUE-fetTime.getTime());
		byte[] rankbt = Bytes.toBytes(rank);
		System.arraycopy(tpbt,0, rowkey, 0, intSize);
		System.arraycopy(fetmbt,0, rowkey, intSize, longSize);
		System.arraycopy(rankbt,0, rowkey, intSize+longSize, intSize);
		return rowkey;
	}

	public static byte[] getRowkey(int type,Date batchTime,int source,int rank){
		byte[] rowkey = new byte[longSize +intSize*3];
		byte[] tpbt = Bytes.toBytes(type);
		byte[] fetmbt = Bytes.toBytes(Long.MAX_VALUE-batchTime.getTime());
		byte[] scbt = Bytes.toBytes(source);
		byte[] rankbt = Bytes.toBytes(rank);
		System.arraycopy(tpbt,0, rowkey, 0, intSize);
		System.arraycopy(fetmbt,0, rowkey, intSize, longSize);
		System.arraycopy(scbt,0, rowkey, intSize+longSize, intSize);
		System.arraycopy(rankbt,0, rowkey, intSize*2+longSize, intSize);
		return rowkey;
	}

}
