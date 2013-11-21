package com.sdata.core;

import java.math.BigDecimal;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;

public class Configuration extends HashMap<String,String>{
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	public Configuration() {
	}
	
	public Configuration(Configuration conf) {
		this.putAll(conf);
	}

	public Long getLong(String key){
		String value = this.get(key);
		if(value==null){
			return null;
		}
		return Long.valueOf(value);
	}
	
	public Integer getInt(String key,int defaultValue){
		String value = this.get(key);
		if(value==null){
			return defaultValue;
		}
		return Integer.valueOf(value);
	}
	
	public BigDecimal getBigDecimal(String key){
		String value = this.get(key);
		if(value==null){
			return null;
		}
		return new BigDecimal(value);
	}
	
	public Date getDate(String key){
		String value = this.get(key);
		if(value==null){
			return null;
		}
		if(value.equals("")){
			return null;
		}
		SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		try {
			return format.parse(value);
		} catch (ParseException e) {
			return null;
		}
	}
	
	public Boolean getBoolean(String key,boolean defaultValue){
		String value = this.get(key);
		if(value==null){
			return defaultValue;
		}
		return Boolean.parseBoolean(value);
	}
	
	public Location getLocation(String key){
		String value = this.get(key);
		if(value==null){
			return null;
		}
		if(value.equals("")){
			return null;
		}
		//1.216332,103.633058
		String[] values = value.split(",");
		if(value==null || values.length!=2){
			return null;
		}
		return new Location(values[0],values[1]);
	}
	
	public String get(String key,String defaultVal){
		String val = this.get(key);
		if(val==null){
			return defaultVal;
		}
		return val;
	}
}