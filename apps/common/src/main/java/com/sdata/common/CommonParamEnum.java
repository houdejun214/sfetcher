package com.sdata.common;


/**
 * @author zhufb
 *
 */
public enum CommonParamEnum  {
	
	URL_PATTERN("url_pattern");
	
	private String name;
	private CommonParamEnum(String name){
		this.name = name;
	}
	
	public String getName() {
		return name;
	}
	
}
