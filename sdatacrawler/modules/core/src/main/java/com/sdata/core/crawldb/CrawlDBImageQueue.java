package com.sdata.core.crawldb;

import java.util.List;
import java.util.Map;

public interface CrawlDBImageQueue {

	public Boolean isImageExists(String url, String source);

	public void insertImageQueue(final List<String> list,String source);

	public void updateImageQueue(String id, String status);
	
	public List<Map<String,Object>> queryImageQueue(int count);
	
	public Map<String,Object> getOneImage();

}