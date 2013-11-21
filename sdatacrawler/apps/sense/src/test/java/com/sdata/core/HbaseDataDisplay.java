package com.sdata.core;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.hbase.thrift.generated.Hbase.Client;
import org.apache.hadoop.hbase.thrift.generated.IOError;
import org.apache.hadoop.hbase.thrift.generated.IllegalArgument;
import org.apache.hadoop.hbase.thrift.generated.TCell;
import org.apache.hadoop.hbase.thrift.generated.TRowResult;
import org.apache.hadoop.hbase.thrift.generated.TScan;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.thrift.TException;

import com.lakeside.core.utils.FileUtils;
import com.nus.next.db.hbase.HNamespaceTable;
import com.nus.next.db.hbase.filter.HBaseFilterBuilder;
import com.nus.next.db.hbase.mapping.CustomSerializer;
import com.nus.next.db.hbase.thrift.BytesBufferUtils;
import com.nus.next.db.hbase.thrift.HBaseClient;
import com.nus.next.db.hbase.thrift.HBaseClient.HBaseThriftActionNoResult;
import com.nus.next.db.hbase.thrift.HBaseClientFactory;
import com.nus.next.db.hbase.thrift.HBaseThriftClient;
import com.nus.next.db.hbase.thrift.HBaseThriftException;
import com.nus.next.db.hbase.thrift.pool.ThriftConnection;
import com.sdata.core.util.ApplicationResourceUtils;
import com.sdata.sense.fetcher.twitter.TwitterFetcher;

public class HbaseDataDisplay {
	
	public static void main(String[] args) throws IOError, TException, IllegalArgument{

//		String id = "qujzRX%2F%2F%2Fr%2BOlkmLBRctlYpAAAA%3D";
//    	HBaseClient client = HBaseClientFactory.getClientWithCustomSeri("next-2","hot");
//    	byte[] base64Decode = EncodeUtils.base64Decode(URLDecoder.decode(id));
//    	Map<String, Map<String, Object>> result = client.queryWithFamily("sense_hot_timeline", base64Decode);
//    	readRecodes()

    	HBaseClient hclient = HBaseClientFactory.getClientWithCustomSeri("next-2","ls");
//    	staticObject(hclient,73l);
    	staticObject(hclient,60l);
    	staticObject(hclient,61l);
//    	HBaseClient hclient2 = HBaseClientFactory.getClientWithCustomSeri("next-1","next");
//    	TScan scan = new TScan();
//    	scan.addToColumns(BytesBufferUtils.buffer("dcf:"));
//    	byte[] buildColumnValueFilter = HBaseFilterBuilder.buildColumnValueFilter("dcf", "type", "=", hclient.getValueSerializer().obj2Byte(8));
//    	scan.setFilterString(buildColumnValueFilter);
//    	Client client = hclient.getThriftClient().getClient();
////		int id = client.scannerOpen(BytesBufferUtils.buffer("hot_image"), BytesBufferUtils.buffer("dcf:"), null, null);
//		int id = client.scannerOpenWithScan(BytesBufferUtils.buffer("hot_image"), scan,null);
//    	List<TRowResult> scannerGetList = client.scannerGetList(id, 2);
//    	Map<String, Object> convertRowResult2Map = hclient.getDataConvertor().convertRowResult2Map(scannerGetList.get(0));
//    	Date d = (Date)convertRowResult2Map.get("batch_time");
//    	System.out.println(d.getTime());
//    	List<TRowResult> scannerGetList2 = hclient2.getThriftClient().getClient().scannerGetList(id, 1);
    	
//    	byte[] l = new byte[8];
//		System.arraycopy(base64Decode, 12, l, 0, 8);
//		long lid = Bytes.toLong(l);
//		Map<String, Object> track = client.query("sense_hot_track", 385272742385025024l);
//		System.out.println(1);
//    	3612772753137796
//    	readRecodes(client, "ls_user_note", "uid", "Violin_lover");
    	
//    	for(long i=29;i<=37;i++){
//    		staticObject(client, i);
//    	}
//    	
//
//    	for(long i=46;i<=52;i++){
//    		staticObject(client, i);
//    	}
    	
	}
	
	private static void staticObject(HBaseClient client,Long objectId){
		statictisCount(client, "twitter_tweets" , objectId);
//		String[] words = getWords(objectId);
//		for(String w:words){
//			statictisPrixCount(client,"tencent_tweets",objectId,"keywords",w.trim());
//		}
//		statictisCount(client, "facebook_posts" , objectId);
//		statictisCount(client, "hardwarezone_posts" , objectId);
//		statictisCount(client, "blogs" , objectId);
	}


	private static void statictisCount(HBaseClient client,
			final String tableName, final Long objectId) {
		final String namespace = "ls";
		final TScan scan = new TScan();
//		scan.addToColumns(ByteBuffer.wrap( Bytes.toBytes("dcf:dtf_oid")));
//		scan.addToColumns(ByteBuffer.wrap( Bytes.toBytes("dcf:pub_time")));
//		scan.addToColumns(ByteBuffer.wrap( Bytes.toBytes("dcf:keywords")));
		
		scan.setFilterString(HBaseFilterBuilder.buildColumnValueFilter("dcf", "dtf_oid", "=",client.getValueSerializer().obj2Byte(objectId)));
		client.execute(new HBaseThriftActionNoResult(){
			public void action(HBaseThriftClient client) throws Exception {
				int scanId = 0;
				try{
					scanId = client.scannerOpenWithScan( BytesBufferUtils.buffer(HNamespaceTable.getTableName(namespace, tableName)), scan, null);
					int result = 0;
					if(scanId>0){
						while(true){
							List<TRowResult> buffer = client.scannerGetList(scanId, 100);
							if(buffer!=null && buffer.size()>0){
								result += buffer.size();
							}else{
								break;
							}
						}
					}
					System.out.println("object:"+objectId+"\t"+tableName+"\t"+result);
				}catch(Exception e){
					throw new HBaseThriftException(e);
				}finally{
					if(scanId >-1){
						try {
							client.scannerClose(scanId);
						} catch (Exception e) {
						}
					}
				}
			}
		});
	}

	private static String[] getWords(Long oid){
		try {
			String resourceUrl = ApplicationResourceUtils.getResourceUrl(TwitterFetcher.class,"conf/".concat(String.valueOf(oid)).concat(".txt"));
			String str = FileUtils.readFileToString(resourceUrl);
			return str.split("\n");
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
	}
	
	private static void statictisPrixCount(HBaseClient client,
			final String tableName, final Long objectId,String column,final Object val) {
		final String namespace = "sense";
		CustomSerializer mp = new CustomSerializer();
		final TScan scan = new TScan();
		scan.addToColumns(ByteBuffer.wrap( Bytes.toBytes("dcf:dtf_oid")));
		scan.addToColumns(ByteBuffer.wrap( Bytes.toBytes("dcf:pub_time")));
		scan.addToColumns(ByteBuffer.wrap( Bytes.toBytes("dcf:keywords")));
		byte[] f1 = HBaseFilterBuilder.buildColumnValueFilter("dcf", "dtf_oid", "=",mp.obj2Byte(objectId));
		byte[] f2 = HBaseFilterBuilder.buildColumnValuePrefixeFilter("dcf", column, mp.obj2Byte(val));
		scan.setFilterString(HBaseFilterBuilder.combineFilter(f1, f2));
		
		client.execute(new HBaseThriftActionNoResult(){
			public void action(HBaseThriftClient client) throws Exception {
				int scanId = 0;
				try{
					scanId = client.scannerOpenWithScan( BytesBufferUtils.buffer(HNamespaceTable.getTableName(namespace, tableName)), scan, null);
					int result = 0;
					if(scanId>0){
						while(true){
							List<TRowResult> buffer = client.scannerGetList(scanId, 100);
							if(buffer!=null && buffer.size()>0){
								result += buffer.size();
							}else{
								break;
							}
						}
					}
					System.out.println(objectId +",object:"+val + "\t"+result);
				}catch(Exception e){
					System.out.println(client);
					e.printStackTrace();
					throw new HBaseThriftException(e);
				}finally{
					if(scanId >-1){
						try {
							client.scannerClose(scanId);
						} catch (Exception e) {
						}
					}
				}
			}
		});
	}

	private static void count(HBaseClient client,
			final String tableName,final String column, final Object value) {
		final String namespace = "sense";
		CustomSerializer mp = new CustomSerializer();
		final TScan scan = new TScan();
		
		byte[] parta = Bytes.toBytes("(SingleColumnValueFilter('dcf','"+column+"', =,'binary:");
		byte[] partb = mp.obj2Byte(value);
		byte[] partc = Bytes.toBytes("',true,true))");
		byte[] filterString = Bytes.add(parta, partb, partc);
		scan.setFilterString(filterString);
		
		client.execute(new HBaseThriftActionNoResult(){
			public void action(HBaseThriftClient client) throws Exception {
				int scanId = 0;
				try{
					scanId = client.scannerOpenWithScan( BytesBufferUtils.buffer(HNamespaceTable.getTableName(namespace, tableName)), scan, null);
					int result = 0;
					if(scanId>0){
						while(true){
							List<TRowResult> buffer = client.scannerGetList(scanId, 100);
							if(buffer!=null && buffer.size()>0){
								result += buffer.size();
							}else{
								break;
							}
						}
					}
					System.out.println("table:"+tableName +",column:"+column +",value:"+value+ ",count:"+result);
				}catch(Exception e){
					throw new HBaseThriftException(e);
				}finally{
					if(scanId >-1){
						try {
							client.scannerClose(scanId);
						} catch (Exception e) {
						}
					}
				}
			}
		});
	}

	private static void readRecodes(HBaseClient client,
			final String tableName,String column, Object value) {
		final String namespace = "ls";
		final CustomSerializer mp = new CustomSerializer();
		final TScan scan = new TScan();
		
		byte[] parta = Bytes.toBytes("(SingleColumnValueFilter('dcf','"+column+"', =,'binary:");
		byte[] partb = mp.obj2Byte(value);
		byte[] partc = Bytes.toBytes("',true,true))");
		byte[] filterString = Bytes.add(parta, partb, partc);
		scan.setFilterString(filterString);

		client.execute(new HBaseThriftActionNoResult(){
			public void action(HBaseThriftClient client) throws Exception {
				int scanId = 0;
				try{
					scanId = client.scannerOpenWithScan( BytesBufferUtils.buffer( HNamespaceTable.getTableName(namespace, tableName)), scan, null);
					if(scanId>0){
						while(true){
							List<TRowResult> buffer = client.scannerGetList(scanId, 100);
							if(buffer.size() == 0){
								break;
							}
							for(TRowResult tr:buffer){
								byte[] b = new byte[4];
								System.arraycopy(tr.getRow(), tr.getRow().length-4, b, 0, 4);
								int int1 = Bytes.toInt(b);
								
								Map<ByteBuffer, TCell> columns = tr.getColumns();
								for(Entry<ByteBuffer, TCell> e:columns.entrySet()){
									String key = BytesBufferUtils.buf2str(e.getKey());
									Object v = mp.byte2Obj(e.getValue().getValue());
									if(key.contains("srct")||key.contains("pub_time")){
										System.out.println(key+":"+v);
									}
									
								}
							}
						}
					}
				}catch(Exception e){
					throw new HBaseThriftException(e);
				}finally{
					if(scanId >-1){
						try {
							client.scannerClose(scanId);
						} catch (Exception e) {
						}
					}
				}
			}
		});
		
	}
	
}