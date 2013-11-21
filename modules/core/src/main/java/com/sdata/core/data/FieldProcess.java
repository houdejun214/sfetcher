package com.sdata.core.data;

import java.util.Map;

import com.sdata.core.Configuration;
import com.sdata.core.parser.html.config.DatumConfig;
import com.sdata.core.parser.html.field.Field;
import com.sdata.solr.IndexControler;

/**
 * 
 * load data column map data
 * 
 * @author houdj
 *
 */
public class FieldProcess {
	private DatumConfig dc;
	private Map<String,Field> fieldMaps;
	private FieldTrans ft;
	private FieldIndex fi;
	
	public FieldProcess(Configuration conf){
		this.dc = DatumConfig.getInstance(conf);
		this.fieldMaps = dc.getFieldMap();
		this.ft = new FieldTrans(conf,dc,fieldMaps);
		this.fi = new FieldIndex(fieldMaps);
	}
	
	public FieldProcess(Configuration conf,String path){
		this.dc = new DatumConfig(path);
		this.fieldMaps =dc.getFieldMap();
		this.ft = new FieldTrans(conf,dc,fieldMaps);
		this.fi = new FieldIndex(fieldMaps);
	}
	
	public Map<String,Object> fieldReduce(Map<String,Object> data){
		return ft.transField(data);
	}

	public void solrIndex(Map<String,Object> data){
		 Map<String, Object> index = fi.getIndexField(data);
		 //THIS SOLR INDEX DONT USE NOW
		 IndexControler.add(index);
	}
	
	public Map<String,Object> elasticIndex(Map<String,Object> data){
		return fi.getIndexField(data);
	}
}