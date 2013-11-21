//package com.sdata.core;
//
//import java.io.File;
//import java.nio.ByteBuffer;
//import java.util.ArrayList;
//import java.util.Collections;
//import java.util.HashMap;
//import java.util.List;
//import java.util.Map;
//
//import net.sf.json.JSONObject;
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
//import com.nus.next.db.hbase.mapping.CustomSerializer;
//import com.nus.next.db.hbase.mapping.ValueSerializer;
//import com.nus.next.db.hbase.thrift.BytesBufferUtils;
//import com.nus.next.db.hbase.thrift.HBaseClient;
//import com.nus.next.db.hbase.thrift.HBaseClientFactory;
//import com.nus.next.db.hbase.thrift.HBaseThriftException;
//import com.nus.next.db.hbase.thrift.pool.ThriftConnection;
//import com.sdata.conf.sites.CrawlConfig;
//import com.sdata.conf.sites.CrawlConfigManager;
//import com.sdata.core.TwitterHotUser.User;
//import com.sdata.core.TwitterHotUser.UserComparator;
//import com.sdata.core.item.CrawlItemDB;
//
///**
// * @author zhufb
// *
// */
//public class HotUserToFileOrDB {
//	
//	private static String table = "thot_type_weibo_top_users";
//	private static String FileName = "news";
//	static CrawlItemDB crawlItemDB;
//	static HBaseClient client;
//	
//	/**
//	 * @param args
//	 * @throws Exception 
//	 */
//	public static void main(String[] args) throws Exception {
//		CrawlConfigManager configs = CrawlConfigManager.load("sense");
//		CrawlConfig crawlSite = configs.getCurCrawlSite();
//		Configuration conf = crawlSite.getConf();
//		conf.put("next.crawler.item.queue.table", "sc_crawl_item_queue_weibo");
//		crawlItemDB = new CrawlItemDB(conf);
//		client = HBaseClientFactory.getClientWithCustomSeri("next-2","thot");
//		ValueSerializer mp = new CustomSerializer();
//		TScan scan = new TScan();
//		scan.addToColumns(ByteBuffer.wrap( Bytes.toBytes("dcf:id")));
//		scan.addToColumns(ByteBuffer.wrap( Bytes.toBytes("dcf:name")));
//		scan.addToColumns(ByteBuffer.wrap( Bytes.toBytes("dcf:follows")));
//		byte[] f1 = HBaseFilterBuilder.buildColumnValueFilter("dcf", "category", "=",mp.obj2Byte("时事新闻"));
//		scan.setFilterString(f1);
//		ThriftConnection thriftClient = client.getThriftConnection();
//		Client hbase = thriftClient.getConnection();
//		int scanId = 0;
//		List<User> list = new ArrayList<User>();
//		UserComparator uc = new UserComparator();
//		int current = 0;
//		try{
//			scanId = hbase.scannerOpenWithScan( BytesBufferUtils.buffer(table), scan, null);
//			if(scanId>0){
//				while(true){
//					List<TRowResult> buffer = hbase.scannerGetList(scanId, 1000);
//					if(buffer!=null && buffer.size()>0){
//						for(TRowResult tr:buffer){
//							Map<ByteBuffer, TCell> columns = tr.getColumns();
//							Object id = mp.byte2Obj(columns.get(BytesBufferUtils.buffer("dcf:id")).getValue());
//							Object name = mp.byte2Obj(columns.get(BytesBufferUtils.buffer("dcf:name")).getValue());
//							Object folct = mp.byte2Obj(columns.get(BytesBufferUtils.buffer("dcf:follows")).getValue());
//							User user = new User(id.toString(),StringUtils.valueOf(name),Long.valueOf(folct.toString()));
//							list.add(user);
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
//		Collections.sort(list, uc);
////		File file = getFile();
//		for(User u:list){
////			saveMysql(u.getId());
//			saveMysqlWeibo(u.getId());
////			FileUtils.writeStringToFile(file, u.getId().toString().concat(":1\r\n"), true);
//		}
//	}
//	
//	
//	private static void saveMysql(Object uid){
//		Map<String,Object> map = new HashMap<String, Object>();
//		map.put("crawlerName", "tencentHot");
//		map.put("objectId", 1);
//		map.put("priorityScore", 100);
//		map.put("status", 0);
//		map.put("entryUrl", "http://t.qq.com");
//		map.put("entryName", "tencent entry");
//		map.put("sourceName", "tencent");
//		map.put("fields", "{}");
//		map.put("objectStatus", "1");
//		JSONObject json = new JSONObject();
//		json.put("dtf_uid", uid);
//		map.put("parameters", json.toString());
//		crawlItemDB.saveCrawlItem(map);
//	}
//	
//
//	private static void saveMysqlWeibo(Object uid){
//		Map<String,Object> map = new HashMap<String, Object>();
//		map.put("crawlerName", "weiboHot");
//		map.put("objectId", 1);
//		map.put("priorityScore", 100);
//		map.put("status", 0);
//		map.put("entryUrl", "http://weibo.com");
//		map.put("entryName", "weibo entry");
//		map.put("sourceName", "weibo");
//		map.put("fields", "{}");
//		map.put("objectStatus", "1");
//		JSONObject json = new JSONObject();
//		json.put("dtf_uid", uid);
//		map.put("parameters", json.toString());
//		crawlItemDB.saveCrawlItem(map);
//	}
//	
//	
//	private static File getFile(){
//		String f = "output/".concat(FileName).concat(".txt");
//		com.lakeside.core.utils.FileUtils.delete(f);
//		com.lakeside.core.utils.FileUtils.insureFileExist(f);
//		return new File(f);
//	}
//	
////	private static Object getUserId( Object value) {
////		return WeiboAPI.getInstance().fetchUserId(value.toString());
//////		
////	}
//	
//}
