package com.sdata.context.config;

import java.math.BigDecimal;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import com.lakeside.core.utils.StringUtils;

public class SdataConfigurable {
	
	static final String SIMPLE_DATA_FORMAT = "yyyy-MM-dd HH:mm:ss";;

	private Configuration conf;

	public SdataConfigurable() {
		super();
	}

	public void setConf(Configuration conf) {
		this.conf = conf;
	}

	public Configuration getConf() {
		return this.conf;
	}

	protected String getConf(String name) {
		return this.conf.get(name);
	}

	protected String getConf(String name, String defaultValue) {
		return this.conf.get(name,defaultValue);
	}

	protected Long getConfLong(String name) {
		String value = this.conf.get(name);
		return Long.valueOf(value);
	}
	
	protected Long getConfLong(String name, Long defaultValue) {
		String value = this.conf.get(name);
		if(!StringUtils.isEmpty(value)){
			return Long.valueOf(value);
		}else{
			return defaultValue;
		}
		
	}

	protected int getConfInt(String name, int defaultValue) {
		String value = this.conf.get(name);
		if(!StringUtils.isEmpty(value)){
			return Integer.valueOf(value);
		}else{
			return defaultValue;
		}
	}

	protected BigDecimal getConfBigDecimal(String name) {
		String value = this.conf.get(name);
		if(!StringUtils.isEmpty(value)){
			return new BigDecimal(value);
		}else{
			return null;
		}
	}
	
	protected Boolean getConfBoolean(String name,Boolean defval) {
		String value = this.conf.get(name);
		if(!StringUtils.isEmpty(value)){
			return Boolean.valueOf(value);
		}else{
			return defval;
		}
	}

	protected Date getConfDate(String name) {
		String value = this.conf.get(name);
		if(!StringUtils.isEmpty(value)){
			SimpleDateFormat format = new SimpleDateFormat(SIMPLE_DATA_FORMAT);
			try {
				return format.parse(value);
			} catch (ParseException e) {
				return null;
			}
		}else{
			return null;
		}
	}

}