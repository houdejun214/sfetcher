package com.sdata.context.state.crawldb;



public enum CrawlDBImageQueueEnum {
	SOURCE("1"),
	URL("2");
	
	private String value;  
	private CrawlDBImageQueueEnum(String v){
		this.value = v;
	}
	
	public String value(){
		return this.value;
	}
}
