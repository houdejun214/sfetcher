package com.sdata.core.data.index.solr;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.impl.HttpSolrServer;

import com.lakeside.core.utils.StringUtils;
import com.lakeside.core.utils.time.DateFormat;
import com.lakeside.core.utils.time.DateTimeUtils;

/**
 * @author zhufb
 *
 */
public class IndexServer {
	
	private IndexSource indexSource;
	private BlockingQueue<Map<String,Object>> queue;
	private int DEFAULT_INDEX_NUM = 200;	
	private int num;
	private IndexStorage is; 
	
	IndexServer(IndexSource indexSource){
		this.indexSource = indexSource;
		SolrServer server = new HttpSolrServer(this.indexSource.getUrl());
		num = indexSource.getNum();
		if(num == 0) {
			num = DEFAULT_INDEX_NUM;
		}
		queue = new LinkedBlockingQueue<Map<String,Object>>(num*10);
		is = new IndexStorage(server);
	}
	
	private String getFilterField(){
		return indexSource.getFilter();
	}
	
	private String getFilterFrom(){
		String from = indexSource.getFrom();
		if(StringUtils.isEmpty(from))
			from = "*";
		return from;
	}

	private String getFilterValue(){
		String value = indexSource.getValue();
		if(StringUtils.isEmpty(value))
			value = "*";
		return value;
	}
	
	private String getFilterTo(){
		String to = indexSource.getTo();
		if(StringUtils.isEmpty(to))
			to = "*";
		return to;
	}
	
	private boolean compareDate(Date date,String s){
		Object d = DateFormat.changeStrToDate(s);
		if(d == null){
			return true;
		}
		return DateTimeUtils.compareDate(date, (Date)d) >= 0 ? true:false;
	}

	private boolean compareLong(Long lng,String s){
		if(!StringUtils.isNum(s)){
			return true;
		}
		Long l = Long.valueOf(s);
		return lng.compareTo(l)>= 0 ? true:false;
	}
	
	private boolean filter(Object object){
		if(object == null){
			return true;
		}
		String value = this.getFilterValue();
		String from = this.getFilterFrom();
		String to = this.getFilterTo();
		boolean bvalue = false;
		boolean bfrom = false;
		boolean bto = false;
		
		if("*".equals(value)){
			bvalue = true;
		}else{
			bvalue =value.equals(StringUtils.valueOf(object));
		}
		
		if("*".equals(from)){
			bfrom = true;
		}else{
			if(object instanceof Date){
				bfrom = this.compareDate((Date)object, from);
			}else if(object instanceof Long){
				bfrom = this.compareLong((Long)object, from);
			}
		}
		
		if("*".equals(to)){
			bto = true;
		}else{
			if(object instanceof Date){
				bto = !this.compareDate((Date)object, to);
			}else if(object instanceof Long){
				bto = !this.compareLong((Long)object, to);
			}
		}
		return bvalue&&bfrom&&bto;
	}
	
	public boolean server(Map<String,Object> map){
		String field = this.getFilterField();
		if(this.filter(map.get(field))){
			this.add(map);
			return true;
		}
		return false;
	}

	public String getServerName(){
		return indexSource.getName();
	}
	
	private void add(Map<String,Object> data ){
		try {
			queue.put(data);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		this.check();
	}
	
	private void check(){
		List<Map<String, Object>> list = null;
		if(size() < num) return;
		synchronized (this) {
			if(size() < num) return;
			list = getQueueList();
			this.save(list);
		}
	}
	
	private void save(List<Map<String,Object>> list){
		is.index(list);
	}

	private List<Map<String,Object>> getQueueList(){
		List<Map<String,Object>> list  = new ArrayList<Map<String,Object>>();
		for(int i=0;i<num;i++){
			Map<String, Object> o = queue.poll();
			if(o == null) break;
			list.add(o);
		}
		return list;
	}
	
	private int size(){
		return queue.size();
	}
	
	public void complete(){
		this.save(getQueueList());
	}
	
}
