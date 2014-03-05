package test;
//package com.sdata.core;
//
//import java.io.File;
//import java.nio.ByteBuffer;
//import java.util.ArrayList;
//import java.util.Collections;
//import java.util.List;
//import java.util.Map;
//
//import org.apache.commons.io.FileUtils;
//import org.apache.hadoop.hbase.thrift.generated.Hbase.Client;
//import org.apache.hadoop.hbase.thrift.generated.TCell;
//import org.apache.hadoop.hbase.thrift.generated.TRowResult;
//import org.apache.hadoop.hbase.thrift.generated.TScan;
//import org.apache.hadoop.hbase.util.Bytes;
//
//import com.lakeside.core.utils.StringUtils;
//import com.nus.next.db.hbase.filter.HBaseFilterBuilder;
//import com.nus.next.db.hbase.mapping.NormalSerializer;
//import com.nus.next.db.hbase.mapping.ValueSerializer;
//import com.nus.next.db.hbase.thrift.BytesBufferUtils;
//import com.nus.next.db.hbase.thrift.HBaseClient;
//import com.nus.next.db.hbase.thrift.HBaseClientFactory;
//import com.nus.next.db.hbase.thrift.HBaseThriftException;
//import com.nus.next.db.hbase.thrift.pool.ThriftConnection;
//import com.sdata.core.TwitterHotUser.User;
//import com.sdata.core.TwitterHotUser.UserComparator;
//
///**
// * @author zhufb
// *
// */
//public class TwitterHotUserHBase {
//	
//	private static int MAX = 5000;
//	/**
//	 * @param args
//	 * @throws Exception 
//	 */
//	public static void main(String[] args) throws Exception {
////		CrawlConfigManager configs = CrawlConfigManager.load("sense");
////		CrawlConfig crawlSite = configs.getCurCrawlSite();
////		CrawlItemDB crawlItemDB = new CrawlItemDB(crawlSite.getConf());
//		
//		HBaseClient client = HBaseClientFactory.getClientWithCustomSeri("next-1","next");
//		ValueSerializer mp = new NormalSerializer();
//		TScan scan = new TScan();
//		scan.addToColumns(ByteBuffer.wrap( Bytes.toBytes("dcf:id")));
//		scan.addToColumns(ByteBuffer.wrap( Bytes.toBytes("dcf:sname")));
//		scan.addToColumns(ByteBuffer.wrap( Bytes.toBytes("dcf:folct")));
//		scan.addToColumns(ByteBuffer.wrap( Bytes.toBytes("dcf:loc")));
//		byte[] filter1 = HBaseFilterBuilder.buildColumnValueFilter("dcf", "loc", "=",mp.obj2Byte("singapore"));
//		byte[] filter2 = HBaseFilterBuilder.buildColumnValueFilter("dcf", "loc", "=",mp.obj2Byte("Singapore"));
//		scan.setFilterString(HBaseFilterBuilder.combineORFilter(filter1, filter2));
//		ThriftConnection thriftClient = client.getThriftConnection();
//		Client hbase = thriftClient.getConnection();
//		int scanId = 0;
//		List<User> list = new ArrayList<User>();
//		UserComparator uc = new UserComparator();
//		int current = 0;
//		try{
//			scanId = hbase.scannerOpenWithScan( BytesBufferUtils.buffer("next_twitter_users"), scan, null);
//			if(scanId>0){
//				while(true){
//					List<TRowResult> buffer = hbase.scannerGetList(scanId, 1000);
//					if(buffer!=null && buffer.size()>0){
//						for(TRowResult tr:buffer){
//							Map<ByteBuffer, TCell> columns = tr.getColumns();
//							Object id = mp.byte2Obj(columns.get(BytesBufferUtils.buffer("dcf:id")).getValue());
//							Object name = mp.byte2Obj(columns.get(BytesBufferUtils.buffer("dcf:sname")).getValue());
//							Object folct = mp.byte2Obj(columns.get(BytesBufferUtils.buffer("dcf:folct")).getValue());
//							User user = new User(Long.valueOf(id.toString()),StringUtils.valueOf(name),Long.valueOf(folct.toString()));
//
//							if(list.size() == MAX&&list.get(MAX-1).getFolct()<user.getFolct()){
//								list.remove(MAX-1);
//							}
//							if(list.size() < MAX){
//								list.add(user);
//								Collections.sort(list, uc);
//							}
//						}
//						current += buffer.size();
//						System.out.println("current size:"+current);
//					}else{
//						break;
//					}
//				}
//			}
//		}catch(Exception e){
//			e.printStackTrace();
//			throw new HBaseThriftException(e);
//		}finally{
//			if(scanId >-1){
//				try {
//					hbase.scannerClose(scanId);
//				} catch (Exception e) {
//				}
//			}
//			thriftClient.close();
//		}
//		
//		File file = getFile();
//		for(User u:list){
//			FileUtils.writeStringToFile(file, u.toString().concat("\r\n"), true);
//		}
//	}
//	
//	private static File getFile(){
//		String f = "output/twitterHotUserHbase.txt";
//		com.lakeside.core.utils.FileUtils.delete(f);
//		com.lakeside.core.utils.FileUtils.insureFileExist(f);
//		return new File(f);
//	}
//}
