package com.sdata.live;

/**
 * @author zhufb
 *
 */
public enum CrawlerEnum {

	Ds("ds"),
	Live("live"),
	History("history");
	
	private String name;
	
	private CrawlerEnum(String name){
		this.name = name;
	}

	public String getName() {
		return name;
	}
}
