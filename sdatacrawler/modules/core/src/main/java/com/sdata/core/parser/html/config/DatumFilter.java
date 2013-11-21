package com.sdata.core.parser.html.config;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;

import com.sdata.core.util.BooleanEvaluator;


/**
 * @author zhufb
 *
 */
public class DatumFilter{
	
	private String nullstr = "";
	private String exists = "";
	private List<String> notNullFields =  new ArrayList<String>();
	private List<String> existsFields = new ArrayList<String>();
	
	public DatumFilter(String filter){
		if(StringUtils.isEmpty(filter)){
			return;
		}
		String[] split = filter.split(",");
		for(String s:split){
			String[] split2 = s.split(":");
			if(split2.length !=2){
				continue;
			}
			if("null".equals(split2[0].toLowerCase())){
				this.setNullStr(split2[1]);
			}else if("exists".equals(split2[0].toLowerCase())){
				this.setExists(split2[1]);
			}
		}
	}
	
	public boolean filter(Map<String,Object> data){
		boolean bNull = true;
		boolean bExists = true;
		String strNull = nullstr;
		String strExists = exists;
		Iterator<String> notNullIter = notNullFields.iterator();
		while(notNullIter.hasNext()){
			String key = notNullIter.next();
			strNull = strNull.replaceAll(key, this.isNull(data, key));
		}
		if(!StringUtils.isEmpty(strNull)){
			bNull = BooleanEvaluator.eval(strNull);
		}
		Iterator<String> existsIter = existsFields.iterator();
		while(existsIter.hasNext()){
			String key = existsIter.next();
			strExists = strExists.replaceAll(key, this.isExists(data, key));
		}

		if(!StringUtils.isEmpty(strExists)){
			bExists = BooleanEvaluator.eval(strExists);
		}
		return bNull&&bExists;
	}
	
	private String isNull(Map<String,Object> data,String key){
		if("0".equals(isExists(data,key))){
			return "0";
		}
		if(data.get(key) == null||"".equals(data.get(key))){
			return "0";
		}
		return "1";
	}
	
	private String isExists(Map<String,Object> data,String key){
		if(!data.containsKey(key)){
			return "0";
		}
		return "1";
	}

	private void setNullStr(String nullStr) {
		this.nullstr = nullStr;
		String[] split = nullStr.split("[\\(\\)\\&\\|]");
		for(String s:split){
			if(!StringUtils.isEmpty(s)){
				notNullFields.add(s);
			}
		}
	}
	
	private void setExists(String exists) {
		this.exists = exists;
		String[] split = exists.split("[\\(\\)\\&\\|]");
		for(String s:split){
			if(!StringUtils.isEmpty(s)){
				existsFields.add(s);
			}
		}
	}
	
}