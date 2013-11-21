package com.sdata.core;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import org.apache.commons.io.FileUtils;

import com.lakeside.core.utils.StringUtils;
import com.lakeside.data.mongo.MongoDBConnectionManager;
import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;

/**
 * @author zhufb
 *
 */
public class TwitterHotUser {
	
	private static int MAX = 5000;
	/**
	 * @param args
	 * @throws Exception 
	 */
	public static void main(String[] args) throws Exception {
//		CrawlConfigManager configs = CrawlConfigManager.load("sense");
//		CrawlConfig crawlSite = configs.getCurCrawlSite();
//		CrawlItemDB crawlItemDB = new CrawlItemDB(crawlSite.getConf());
		
		DB db = MongoDBConnectionManager.getConnection("172.18.109.18", 27027,"twitterOthers", null, null);
		DBCollection collection = db.getCollection("users");
		BasicDBObject query = new BasicDBObject();
		BasicDBList locs = new BasicDBList();
		locs.add(new BasicDBObject("loc","Singapore"));
		locs.add(new BasicDBObject("loc", "singapore"));
		query.append("$or", locs);
		DBCursor cursor = collection.find(query);
		List<User> list = new ArrayList<User>();
		UserComparator uc = new UserComparator();
		while(cursor.hasNext()){
			DBObject next = cursor.next();
			String v = StringUtils.valueOf(next.get("folct"));
			Long folct = 0L;
			if(StringUtils.isNum(v)){
				folct = Long.valueOf(v);
			}else{
				v = StringUtils.valueOf(next.get("folc"));
				if(StringUtils.isNum(v)){
					folct = Long.valueOf(v);
				}
			}
			Long id = Long.valueOf(next.get("id").toString());
			String name = StringUtils.valueOf(next.get("sname"));
			
			User user = new User(id,name,folct);
//			BeanUtils.copyProperties(user,next.toMap());
			
			if(list.size() == MAX&&list.get(MAX-1).getFolct()<user.getFolct()){
				list.remove(MAX-1);
			}
			if(list.size() < MAX){
				list.add(user);
				Collections.sort(list, uc);
			}
		}
		File file = getFile();
		for(User u:list){
			FileUtils.writeStringToFile(file, u.toString().concat("\r\n"), true);
		}
	}
	
	private static File getFile(){
		String f = "output/twitterHotUser.txt";
		com.lakeside.core.utils.FileUtils.delete(f);
		com.lakeside.core.utils.FileUtils.insureFileExist(f);
		return new File(f);
	}
	
	static class UserComparator implements Comparator<User>{
		public int compare(User o1, User o2) {
			return o2.getFolct().compareTo(o1.getFolct());
		}
	}
	
	static class User{
		private Object id;
		private String name;
		private Long folct;
		private Long frdct;
		private Long stact;
		public User(Object id,String name,Long folct){
			this.id = id;
			this.name = name;
			this.folct = folct;
		}
		public Object getId() {
			return id;
		}
		public void setId(Long id) {
			this.id = id;
		}
		public String getName() {
			return name;
		}
		public void setName(String name) {
			this.name = name;
		}
		public Long getFolct() {
			return folct;
		}
		public void setFolct(Long folct) {
			this.folct = folct;
		}
		public Long getFrdct() {
			return frdct;
		}
		public void setFrdct(Long frdct) {
			this.frdct = frdct;
		}
		public Long getStact() {
			return stact;
		}
		public void setStact(Long stact) {
			this.stact = stact;
		}
		
		public String toString(){
			return "{uid"+":"+id+",uname:"+name+",followers:"+folct+"}";
		}
	}
}
