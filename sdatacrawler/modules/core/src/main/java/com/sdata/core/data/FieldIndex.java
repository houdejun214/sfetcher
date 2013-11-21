package com.sdata.core.data;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import com.sdata.core.data.index.IndexDataHandle;
import com.sdata.core.parser.html.field.DatumField;
import com.sdata.core.parser.html.field.DatumItem;
import com.sdata.core.parser.html.field.DatumList;
import com.sdata.core.parser.html.field.Field;

/**
 * @author zhufb
 *
 */
public class FieldIndex {
	
	private final String ID = "id";
	private final String ORIGIN = "origin";
	private final String DBID = "dbid";
	private Map<String,Field> fieldMaps = new HashMap<String,Field>();
	
	public FieldIndex(Map<String,Field> fieldMaps){
		this.fieldMaps = fieldMaps;
	}	
	
	public Map<String,Object> getIndexField(Map<String,Object> data){
		Map<String,Object> result = new HashMap<String,Object>();
		if(this.fieldMaps.size()< 0 ){
			return result;
		}
		Iterator<String> iterator = this.fieldMaps.keySet().iterator();
		while(iterator.hasNext()){
			String name = iterator.next();
			Field field = this.fieldMaps.get(name);
			this.putIndexData(field, data, result);
		}
		this.putIndexID(result);
		
		return result;
	}

	private void putIndexData(Field f,Object data,Map<String,Object> result){
		if(data == null){
			return;
		}
		if(f instanceof DatumList){
			if(!(data instanceof Map)){
				return;
			}
			Object list = ((Map)data).get(f.getName());
			if(!(list instanceof List)){
				return;
			}
			this.putIndexData((DatumList)f, (List)list, result);
		}else if(f instanceof DatumItem){
			this.putIndexData((DatumItem)f, data, result);
		}else if(f instanceof DatumField){
			if(!(data instanceof Map)){
				return;
			}
			this.putIndexData((DatumField)f, (Map)data, result);
		}
	}
	
	private void putIndexData(DatumField field,Map<String,Object> data,Map<String,Object> result){
		if(data == null){
			return;
		}
		Object value = data.get(field.getName());
		//check if put index data
		IndexDataHandle.handle(result, field.getIndex(), value);
		
		Iterator<Field> childs = field.getChilds().iterator();
		while(childs.hasNext()){
			this.putIndexData(childs.next(), value, result);
		}

		String indexOnly = field.getIndexOnly();
		if("true".equals(indexOnly)){
			data.remove(field.getName());
		}
	}
	
	private void putIndexData(DatumList dl,List listData,Map<String,Object> result){
		if(listData == null||listData.size() == 0){
			return;
		}
		Object value = listData.get(0);
		//check if put index data
		IndexDataHandle.handle(result, dl.getIndex(), value);
		
		Iterator<Field> iterator = dl.getChilds().iterator(); 
		while(iterator.hasNext()){
			this.putIndexData(iterator.next(), value, result);
		}
	}
	

	private void putIndexData(DatumItem di,Object data,Map<String,Object> result){
		if(data == null){
			return;
		}
		//check if put index data
		if(!(data instanceof Map)){
			IndexDataHandle.handle(result, di.getIndex(), data);
			return;
		}
		Iterator<Field> iterator = di.getChilds().iterator();
		while(iterator.hasNext()){
			this.putIndexData(iterator.next(), data, result);
		}
	}
	
	private void putIndexID(Map<String,Object> indexData){
		Object origin = indexData.get(ORIGIN);
		Object dbid = indexData.get(DBID);
		if(origin==null||dbid==null){
			throw new RuntimeException("analysis index error,origin or dbid is null ,origin:"+origin+",dbid:"+dbid);
		}
		indexData.put(ID, String.valueOf(origin).concat("_").concat(String.valueOf(dbid)));
	}
}
