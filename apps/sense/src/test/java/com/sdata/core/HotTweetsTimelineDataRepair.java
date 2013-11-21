//package com.sdata.core;
//
//import java.nio.ByteBuffer;
//import java.util.HashMap;
//import java.util.List;
//import java.util.Map;
//
//import net.sf.json.JSONObject;
//
//import org.apache.hadoop.hbase.thrift.generated.Hbase.Client;
//import org.apache.hadoop.hbase.thrift.generated.TCell;
//import org.apache.hadoop.hbase.thrift.generated.TRowResult;
//import org.apache.hadoop.hbase.thrift.generated.TScan;
//import org.apache.hadoop.hbase.util.Bytes;
//
//import com.lakeside.core.utils.StringUtils;
//import com.nus.next.db.hbase.thrift.BytesBufferUtils;
//import com.nus.next.db.hbase.thrift.HBaseClient;
//import com.nus.next.db.hbase.thrift.HBaseClientFactory;
//import com.nus.next.db.hbase.thrift.HBaseThriftException;
//import com.tencent.weibo.api.TAPI;
//import com.tencent.weibo.constants.OAuthConstants;
//import com.tencent.weibo.oauthv1.OAuthV1;
//
///**
// * @author zhufb
// *
// */
//public class HotTweetsTimelineDataRepair {
//	private static OAuthV1 oauth;
//	private static TAPI api;
//	
//	public static void main(String[] args) throws Exception {
//		repairRawData();
//	}
//	
//	static void repairTimeline(){
//		TencentOAuthV1.init();
//		HBaseClient client = HBaseClientFactory.getClientWithCustomSeri("next-2","sense");
//		Client hbase = client.getThriftConnection().getConnection();
//		TScan scan = new TScan();
//		int scanId = 0;
//		int count = 0;
//		try{
//			scanId = hbase.scannerOpenWithScan(BytesBufferUtils.buffer("sense_hot_timeline"), scan, null);
//			if(scanId>0){
//				while(true){
//					List<TRowResult> buffer = hbase.scannerGetList(scanId, 100);
//					if(buffer==null||buffer.size() == 0){
//						break;
//					}
//					for(TRowResult tr:buffer){
//						byte[] rk = tr.getRow();
//						Map<ByteBuffer, TCell> columns = tr.getColumns();
//						TCell tCell = columns.get(BytesBufferUtils.buffer("dcf:uname"));
//						String uname = client.getValueSerializer().byte2Obj(tCell.getValue()).toString();
//						
//						TCell tidCell = columns.get(BytesBufferUtils.buffer("dcf:id"));
//						Object otid = client.getValueSerializer().byte2Obj(tidCell.getValue());
//						String tid = otid.toString();
//						
//						if(uname.indexOf("�")<0){
//							continue;
//						}
//						count++;
//						byte[] l = new byte[8];
//						System.arraycopy(rk, 12, l, 0, 8);
//						long lid = Bytes.toLong(l);
//						boolean isRepair = false;
//						Map<String, Object> track = client.query("sense_hot_track", lid);
//						String unameTrack  = track.get("uname").toString();
//						if(unameTrack.indexOf("�")<0){
//							Map<String,Object> timelineMap = new HashMap<String, Object>();
//							timelineMap.put("content",track.get("content"));
//							timelineMap.put("uname",unameTrack);
//							timelineMap.put("loc",track.get("loc"));
//							client.save("sense_hot_timeline", rk, timelineMap);
//							isRepair = true;
//						}
//						
//						if(!isRepair){
//							String content = api.show(oauth, "json", tid);
//							JSONObject JSONObj = JSONObject.fromObject(content);
//							JSONObject data = JSONObj.getJSONObject("data");
//							Map<String,Object> map = new HashMap<String, Object>();
//							map.put("content", data.get("text"));
//							map.put("uname", data.get("nick"));
//							map.put("loc", data.get("location"));
//							client.save("sense_hot_timeline", rk, map);
//							client.save("sense_hot_track", lid, map);
//							client.save("sense_hot_track_all", lid, map);
//							client.save("sense_hot_tencent_tweets", lid, map);
//						}
//						System.out.println(count+":id-"+lid);
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
//			client.getThriftConnection().close();
//		}
//	}
//	
//	static void repairTrack(){
//		TencentOAuthV1.init();
//		HBaseClient client = HBaseClientFactory.getClientWithCustomSeri("next-2","sense");
//		Client hbase = client.getThriftConnection().getConnection();
//		TScan scan = new TScan();
//		int scanId = 0;
//		int count = 0;
//		try{
//			scanId = hbase.scannerOpenWithScan(BytesBufferUtils.buffer("sense_hot_track"), scan, null);
//			if(scanId>0){
//				while(true){
//					List<TRowResult> buffer = hbase.scannerGetList(scanId, 100);
//					if(buffer==null||buffer.size() == 0){
//						break;
//					}
//					for(TRowResult tr:buffer){
//						byte[] rk = tr.getRow();
//						Map<ByteBuffer, TCell> columns = tr.getColumns();
//						TCell tCell = columns.get(BytesBufferUtils.buffer("dcf:uname"));
//						if(tCell == null){
//							System.out.println("uname is null");
//						}
//						if(tCell != null){
//							String uname = StringUtils.valueOf(client.getValueSerializer().byte2Obj(tCell.getValue()));
//							if(!StringUtils.isEmpty(uname)&&uname.indexOf("�")<0){
//								continue;
//							}
//						}
//
//						TCell tidCell = columns.get(BytesBufferUtils.buffer("dcf:id"));
//						Object otid = client.getValueSerializer().byte2Obj(tidCell.getValue());
//						String tid = otid.toString();
//						long lid = Bytes.toLong(rk);
//						count++;
//						String content = api.show(oauth, "json", tid);
//						JSONObject JSONObj = JSONObject.fromObject(content);
//						JSONObject data = JSONObj.getJSONObject("data");
//						if(data.isNullObject()){
//							System.out.println(lid+" is null");
//							continue;
//						}
//						Map<String,Object> map = new HashMap<String, Object>();
//						map.put("content", data.get("text"));
//						map.put("uname", data.get("nick"));
//						map.put("loc", data.get("location"));
//						client.save("sense_hot_track", lid, map);
//						client.save("sense_hot_track_all", lid, map);
//						client.save("sense_hot_tencent_tweets", lid, map);
//						System.out.println(count+":id-"+lid);
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
//			client.getThriftConnection().close();
//		}
//	}
//	
//
//	static void repairRawData(){
//		TencentOAuthV1.init();
//		HBaseClient client = HBaseClientFactory.getClientWithCustomSeri("next-2","sense");
//		Client hbase = client.getThriftConnection().getConnection();
//		TScan scan = new TScan();
//		int scanId = 0;
//		int count = 0;
//		try{
//			scanId = hbase.scannerOpenWithScan(BytesBufferUtils.buffer("sense_hot_tencent_tweets"), scan, null);
//			if(scanId>0){
//				while(true){
//					List<TRowResult> buffer = hbase.scannerGetList(scanId, 100);
//					if(buffer==null||buffer.size() == 0){
//						break;
//					}
//					for(TRowResult tr:buffer){
//						byte[] rk = tr.getRow();
//						Map<ByteBuffer, TCell> columns = tr.getColumns();
//						TCell tCell = columns.get(BytesBufferUtils.buffer("dcf:uname"));
//						if(tCell == null){
//							System.out.println("uname is null");
//						}
//						if(tCell != null){
//							String uname = StringUtils.valueOf(client.getValueSerializer().byte2Obj(tCell.getValue()));
//							if(!StringUtils.isEmpty(uname)&&uname.indexOf("�")<0){
//								continue;
//							}
//						}
//
//						TCell tidCell = columns.get(BytesBufferUtils.buffer("dcf:id"));
//						Object otid = client.getValueSerializer().byte2Obj(tidCell.getValue());
//						String tid = otid.toString();
//						long lid = Bytes.toLong(rk);
//						count++;
//						String content = api.show(oauth, "json", tid);
//						JSONObject JSONObj = JSONObject.fromObject(content);
//						JSONObject data = JSONObj.getJSONObject("data");
//						if(data.isNullObject()){
//							System.out.println(lid+" is null");
//							continue;
//						}
//						Map<String,Object> map = new HashMap<String, Object>();
//						map.put("content", data.get("text"));
//						map.put("uname", data.get("nick"));
//						map.put("loc", data.get("location"));
//						client.save("sense_hot_tencent_tweets", lid, map);
//						System.out.println(count+":id-"+lid);
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
//			client.getThriftConnection().close();
//		}
//	}
//	
//	
//	static class TencentOAuthV1{
//		public static void init(){
//			if(api == null){
//				synchronized (HotTweetsTimelineDataRepair.class) {
//					if(api == null){
//						String consumerKey = "801126559";
//						String consumerSecret ="2eb966cecf339076941dd7ffd383cd90";
//						String AccessToken = "8204f6c9a6e04428b43e3f419b4faaa9";
//						String AccessTokenSecret = "b336aa99fdb52490bf2fda8bce0dcbdc";
//						oauth = new OAuthV1();
//						oauth.setOauthConsumerKey(consumerKey);
//						oauth.setOauthConsumerSecret(consumerSecret);
//						oauth.setOauthToken(AccessToken);
//						oauth.setOauthTokenSecret(AccessTokenSecret);
//						api = new TAPI(OAuthConstants.OAUTH_VERSION_1);
//					}
//				}
//			}
//		}
//	}
//}
