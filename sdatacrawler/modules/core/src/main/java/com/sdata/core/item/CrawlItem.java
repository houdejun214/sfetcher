package com.sdata.core.item;

import java.util.Date;
import java.util.Map;

import com.lakeside.core.utils.MapUtils;

/**
 * 
 * Crawl item for crawler from SCMS
 * @author zhufb
 *
 */
public class CrawlItem {
	
	public CrawlItem(Map<String,Object> map){
		if(map == null||map.size() == 0){
			return;
		}
		this.id = MapUtils.getLong(map,"id");
		this.keyword = MapUtils.getString(map, "keyword");
		this.type = MapUtils.getString(map,"type");
        if(map.containsKey("start")&&map.get("start") instanceof Date){
        	this.start =(Date)map.get("start");
		}
        if(map.containsKey("end")&&map.get("end") instanceof Date){
        	this.end =(Date)map.get("end");
		}
		this.status = MapUtils.getString(map,"status");
	}
	
	private Long id;
	private String keyword;
	private String type;
	private Date start;
	private Date end;
	private String  status;
	public Long getId() {
		return id;
	}
	public void setId(Long id) {
		this.id = id;
	}
	public String getKeyword() {
		return keyword;
	}
	public void setKeyword(String keyword) {
		this.keyword = keyword;
	}
	public String getType() {
		return type;
	}
	public void setType(String type) {
		this.type = type;
	}
	public Date getStart() {
		return start;
	}
	public void setStart(Date start) {
		this.start = start;
	}
	public Date getEnd() {
		return end;
	}
	public void setEnd(Date end) {
		this.end = end;
	}
	public String getStatus() {
		return status;
	}
	public void setStatus(String status) {
		this.status = status;
	}
	
	
}
