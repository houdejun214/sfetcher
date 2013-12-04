package com.sdata.core.index.solr;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import com.sdata.context.config.Configuration;
import com.sdata.core.parser.html.field.Field;
import com.sdata.core.parser.html.field.datum.DatumField;
import com.sdata.solr.IndexDataHandle;

/**
 * @author zhufb
 *
 */
public class SolrGenerater {
	
	private final String ID = "id";
	private final String ORIGIN = "origin";
	private final String DBID = "dbid";
	private Map<String,Field> map;
	
	public SolrGenerater(Configuration config,Map<String,Field> indexs){
		this.map = indexs;
	}
	
	public Map<String,Object> gengerate(Map<String,Object> data){
		Map<String,Object> result = new HashMap<String,Object>();
		if(this.map.size() <= 0){
			return null;
		}
		//data -> result
		this.gen(data, result);
		//indexMap -> result
		this.analysisIndex(result);
		return result;
	}

	
	private void analysisIndex(Map<String,Object> result){
		Iterator<String> iterator = map.keySet().iterator();
		while(iterator.hasNext()){
			String key = iterator.next();
			DatumField field =(DatumField) map.get(key);
			Object index = field.getIndex();
			Object value = field.getValue();
			if(value!=null&&index!=null){
				result.put(index.toString(), value);
			}
		}
		Object origin = result.get(ORIGIN);
		Object dbid = result.get(DBID);
		if(origin==null||dbid==null){
			throw new RuntimeException("analysis index error,origin or dbid is null ,origin:"+origin+",dbid:"+dbid);
		}
		result.put(ID, String.valueOf(origin).concat("_").concat(String.valueOf(dbid)));
	}
	
	private void gen(Map<String,Object> obj,Map<String,Object> result){
		Iterator<Entry<String, Object>> iterator = obj.entrySet().iterator();
		while(iterator.hasNext()){
			Entry<String, Object> next = iterator.next();
			String key = next.getKey();
			Object value = next.getValue();
			if(value instanceof Map){
				gen((Map)value,result);
			}else if(value instanceof List){
				genList((List)value,result);
			}
			if(this.map.containsKey(key)){
				DatumField field = (DatumField)map.get(key);
				Object index = field.getIndex();
				if(index == null){
					continue;
				}
				//handle the data 
				IndexDataHandle.handle(result,index.toString(), value);
			}
		}
	}

	private void genList(List<?> list,Map<String,Object> result) {
		for(Object item:list){
			if(item instanceof Map){
				gen((Map)item,result);
			}else if(item instanceof List){
				genList((List)item,result);
			}
		}
	}
}
