package com.sdata.live.fetcher.tencent;

import java.util.Date;

import org.apache.commons.lang.StringUtils;

/**
 * @author zhufb
 *
 */
public class TencentCrawlState {

	public TencentCrawlState(){
		
	}
	public TencentCrawlState(int page){
		this.page = page;
	}

	public TencentCrawlState(Date start,Date end){
		this.start = start;
		this.end = end;
	}
	
	public TencentCrawlState(int page,Date start,Date end){
		this.page= page;
		this.start = start;
		this.end = end;
	}
	private String pageTime;
	private String lastId;
	private Integer page;
	private Date start;
	private Date end;
	public int getPage() {
		return page;
	}
	public void setPage(int page) {
		this.page = page;
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

	public String getLastId() {
		return lastId;
	}

	public void setLastId(String lastId) {
		this.lastId = lastId;
	}

	public String getPageTime() {
		return pageTime;
	}

	public void setPageTime(String pageTime) {
		this.pageTime = pageTime;
	}
	
	@Override
	public String toString(){
		StringBuffer sb = new StringBuffer();
		sb.append("{");
		if(start!=null){
			sb.append("startTime:").append(start).append(",");
		}
		if(end!=null){
			sb.append("endTime:").append(end).append(",");
		}
		if(page!=null){
			sb.append("page:").append(page).append(",");
		}
		if(!StringUtils.isEmpty(lastId)){
			sb.append("lastId:").append(lastId).append(",");
		}
		if(!StringUtils.isEmpty(pageTime)){
			sb.append("pageTime:").append(pageTime).append(",");
		}
		sb.replace(sb.length()-1, sb.length(), "}");
		return sb.toString();
		
	}
}
