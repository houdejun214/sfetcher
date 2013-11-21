package com.sdata.component.site;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import net.sf.json.JSONArray;
import net.sf.json.JSONObject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.lakeside.core.utils.StringUtils;
import com.lakeside.core.utils.UUIDUtils;
import com.mongodb.DBObject;
import com.sdata.component.data.dao.TopicMgDao;
import com.sdata.core.Configuration;
import com.sdata.core.Constants;
import com.sdata.core.FetchDatum;
import com.sdata.core.RunState;
import com.sdata.core.parser.ParseResult;
import com.sdata.core.util.ApplicationContextHolder;

/**
 * topic api implement
 * 
 * @author zhufb
 *
 */
public class TopicAPI {
	
	private static final Logger log = LoggerFactory.getLogger("TopicAPI");
	private TopicMgDao topicDao;
	
	public TopicAPI(Configuration conf,RunState state) {
		String host = conf.get("mongoHost");
		int port = conf.getInt("mongoPort",27017);
		String dbName = conf.get("mongoDbName");
		topicDao = new TopicMgDao();
		topicDao.initilize(host,port,dbName);
	}

	/**
	 * fetch one tweet by id
	 * 
	 * @param id
	 * @return
	 */
	/**
	 * parse current array 
	 * 
	 * @param array
	 * @return
	 */
	@SuppressWarnings("unchecked")
	public List parse(List array) {
		if(array==null||array.size() ==0 ) {
			return array;
		}
		List result = new ArrayList();
		ComparatorDown cdown = new ComparatorDown();
		//1 query database 
		List<DBObject> dbData = topicDao.query();
		//2 update database and add db data to new array
		Iterator<DBObject> iterator = dbData.iterator();
		while(iterator.hasNext()){
			DBObject next = iterator.next();
			Map json = next.toMap();
			Collections.sort(array,cdown);
			int index = Collections.binarySearch(array, json, cdown);
			if(index >= 0) {
				json = (Map) array.remove(index);
			}else{
				this.putEndDate(json);
			}
			result.add(json);
		}

		//3 add new begin topics
		Iterator<Map> iterator2 = array.iterator();
		while(iterator2.hasNext()){
			Map next = iterator2.next();
			Long id = (Long)next.get(Constants.TOPIC_ID);
			Map<?, ?> one = topicDao.queryOne(id);
			if(one != null) {
				next.put(Constants.TOPIC_DATE_LIST, one.get(Constants.TOPIC_DATE_LIST));
			}else{
				next.put(Constants.FETCH_TIME, new Date());
			}
			this.putBeginDate(next);
			result.add(next);
		}
		return result;
	}	
	
	public List<FetchDatum> parseTwitterTopic(List<FetchDatum> fetchList) {
		List<Map<String, Object>> array= new ArrayList<Map<String, Object>>();
		for(int i=0;i<fetchList.size();i++){
			FetchDatum datum = fetchList.get(i);
			array.add(datum.getMetadata());
		}
		
		List<FetchDatum> result = new ArrayList<FetchDatum>();
		if(array==null||array.size() ==0 ) {
			return result;
		}
		ComparatorDBForTwi cdown = new ComparatorDBForTwi();
		//1 query database 
		List<DBObject> dbData = topicDao.query();
		//2 update database and add db data to new array
		Iterator<DBObject> iterator = dbData.iterator();
		while(iterator.hasNext()){
			DBObject next = iterator.next();
			Map<String,Object> json = next.toMap();
			Collections.sort(array,cdown);
			int index = Collections.binarySearch(array, json, cdown);
			if(index >= 0) {
				json = (Map<String,Object>) array.remove(index);
			}else{
				this.putEndDate(json);
			}
			FetchDatum datum = new FetchDatum();
			datum.setMetadata(json);
			result.add(datum);
		}

		//3 add new begin topics
		Iterator<Map<String, Object>> iterator2 = array.iterator();
		while(iterator2.hasNext()){
			Map<String, Object> next = iterator2.next();
			String topicName =StringUtils.valueOf(next.get(Constants.TOPIC_NAME)) ;
			String locKey = "loc";
			if(next.containsKey(Constants.TOPIC_LOCATION)){
				locKey = Constants.TOPIC_LOCATION;;
			}
			String locName =(String) ((Map)next.get(locKey)).get("locname");
			
			UUID id = UUIDUtils.getMd5UUID(topicName+locName);
			Map<?, ?> one = topicDao.queryOne(id);
			if(one != null) {
				next.put(Constants.TOPIC_DATE_LIST, one.get(Constants.TOPIC_DATE_LIST));
			}else{
				next.put(Constants.FETCH_TIME, new Date());
			}
			this.putBeginDate(next);
			FetchDatum datum = new FetchDatum();
			datum.setMetadata(next);
			result.add(datum);
		}
		return result;
	}	
	
	public List<FetchDatum> parseTencentTopic(List<FetchDatum> fetchList) {
		List<Map<String, Object>> array= new ArrayList<Map<String, Object>>();
		for(int i=0;i<fetchList.size();i++){
			FetchDatum datum = fetchList.get(i);
			array.add(datum.getMetadata());
		}
		
		List<FetchDatum> result = new ArrayList<FetchDatum>();
		ComparatorDown cdown = new ComparatorDown();
		//1 query database 
		List<DBObject> dbData = topicDao.query();
		//2 update database and add db data to new array
		Iterator<DBObject> iterator = dbData.iterator();
		while(iterator.hasNext()){
			DBObject next = iterator.next();
			Map json = next.toMap();
			Collections.sort(array,cdown);
			int index = Collections.binarySearch(array, json, cdown);
			if(index >= 0) {
				json = (Map) array.remove(index);
			}else{
				this.putEndDate(json);
			}
			FetchDatum datum = new FetchDatum();
			datum.setMetadata(json);
			result.add(datum);
		}

		//3 add new begin topics
		Iterator<Map<String, Object>> iterator2 = array.iterator();
		while(iterator2.hasNext()){
			Map next = iterator2.next();
			String id =(String)next.get(Constants.TOPIC_ID);
			Map<?, ?> one = topicDao.queryOne(UUIDUtils.getMd5UUID(id));
			if(one != null) {
				next.put(Constants.TOPIC_DATE_LIST, one.get(Constants.TOPIC_DATE_LIST));
			}else{
				next.put(Constants.FETCH_TIME, new Date());
			}
			this.putBeginDate(next);
			FetchDatum datum = new FetchDatum();
			datum.setMetadata(next);
			result.add(0,datum);
		}
		return result;
	}	

	private void putBeginDate(Map<String, Object> json){
		if(json == null) {
			return;
		}
		Object object = json.get(Constants.TOPIC_DATE_LIST);
		List dl = new ArrayList();
		if(object instanceof List){
			dl = (List)object;
		}
		List<Date> date = new  ArrayList<Date>();
		date.add(new Date());
		dl.add(date);
		json.put(Constants.TOPIC_DATE_LIST,dl);
		json.put(Constants.TOPIC_STATE, Constants.TOPIC_STATE_START);
	}
	
	private void putEndDate(Map<String, Object> json){
		if(json == null) {
			return;
		}
		Object object = json.get(Constants.TOPIC_DATE_LIST);
		if(object == null||!(object instanceof List)){
			return;
		}
		List dl = (List)object;
		int index =  dl.size() - 1;
		((List)dl.get(index)).add(new Date());
		json.put(Constants.TOPIC_DATE_LIST, dl);
		json.put(Constants.TOPIC_STATE, Constants.TOPIC_STATE_END);
	}
	
	
	/**
	 * class web data comparator
	 * 
	 * @author zhufb
	 *
	 */
	class ComparatorDown implements Comparator{
		 public int compare(Object arg0, Object arg1) {
			String topic1 = ((Map)arg0).get(Constants.TOPIC_ID).toString();
			String topic2 = ((Map)arg1).get(Constants.TOPIC_ID).toString();
			return topic1.compareTo(topic2);
		 }
	}

	class ComparatorDBForTwi implements Comparator{
		public int compare(Object arg0, Object arg1) {
			String locKey1 = "loc";
			String locKey2 = "loc";
			if(((Map)arg0).containsKey(Constants.TOPIC_LOCATION)){
				locKey1 = Constants.TOPIC_LOCATION;;
			}
			if(((Map)arg1).containsKey(Constants.TOPIC_LOCATION)){
				locKey2 = Constants.TOPIC_LOCATION;;
			}
			String locStr1 =(String) ((Map)((Map)arg0).get(locKey1)).get("locname");
			String locStr2 =(String) ((Map)((Map)arg1).get(locKey2)).get("locname");
			String topic1 = ((Map)arg0).get(Constants.TOPIC_NAME).toString()+locStr1;
			String topic2 = ((Map)arg1).get(Constants.TOPIC_NAME).toString()+locStr2;
			return topic1.compareTo(topic2);
		 }
	}

}
