package com.sdata.core.data.trans;

import java.util.Map;

import com.sdata.context.config.Configuration;
import com.sdata.core.parser.config.DatumConfig;
import com.sdata.core.parser.html.field.Field;

/**
 * raw data optimizer before saving to database
 * 
 * @author zhufb
 *
 */
public class DataOptimizer {
	
	private DataReducer dataReducer;
	
	public DataOptimizer(Configuration conf){
		DatumConfig dc = DatumConfig.getInstance(conf);
		Map<String,Field>  fieldMaps = dc.getFieldMap();
		this.dataReducer = new DataReducer(conf,dc,fieldMaps);
	}
	
	/**
	 * data optimize 
	 * 
	 * @param data
	 * @return
	 */
	public Map<String,Object> optimize(Map<String,Object> data){
		// now only reduce operate
		return dataReducer.reduce(data);
	}
}