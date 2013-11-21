package com.sdata.core.data;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.List;
import java.util.Map;

import net.sf.json.JSONNull;

import com.sdata.core.Configuration;
import com.sdata.core.Constants;
import com.sdata.core.parser.html.config.DatumConfig;
import com.sdata.core.parser.html.context.DatumContext;
import com.sdata.core.parser.html.field.DatumField;
import com.sdata.core.parser.html.field.Field;

/**
 * Field trans and make images list
 * 
 * 
 * @author zhufb
 *
 */
public class FieldTrans {
	
	private Configuration conf;
	private DatumConfig dc;
	private Map<String,Field> fieldMaps;
	public FieldTrans(Configuration conf,DatumConfig dc,Map<String,Field> fieldMaps){
		this.conf = conf;
		this.dc = dc;
		this.fieldMaps = fieldMaps;
	}	

	public Map<String,Object> transField(Map<String,Object> data){
		if(fieldMaps == null||this.fieldMaps.size()<=0){
			return data;
		}
		List<Field> list = new ArrayList<Field>(this.fieldMaps.values());
		Collections.sort(list,new FieldComparator());
		DatumContext context = new DatumContext(this.conf);
		for(Field field:list){
			Object value = ((DatumField)field).transData(context,data);
			if(value != null&&!(value instanceof JSONNull)){
				// if null return null?
				context.addData(field.getName(), value);
			}
		}
		this.putFetchTime(data);
		
		// notify
		this.notify(data);
		//filter
		if(!filterData(context.getMetadata())){
			return null;
		}
		return context.getMetadata();
	}
	
	private void putFetchTime(Map<String,Object> data){
		data.put(Constants.FETCH_TIME, new Date());
	}
	
	private boolean filterData(Map<String,Object> data){
		return this.dc.getDatumFilter().filter(data);
	}

	private void notify(Map<String,Object> data){
		this.dc.getCrawlNotify().notify(data);
	}

	class FieldComparator implements Comparator{
		public int compare(Object arg0, Object arg1) {
			DatumField field0 = (DatumField)arg0;
			DatumField field1 = (DatumField)arg1;
			Integer f0 = 0;
			Integer f1 = 0;
			if(field0.hasChild()){
				f0 = 1;
			}
			if(field1.hasChild()){
				f1 = 1;
			}
			return f0.compareTo(f1);
		 }
	}
	
}