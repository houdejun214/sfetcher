package com.sdata.core.site;

import java.util.Date;

import com.lakeside.core.utils.time.DateTimeUtils;
import com.sdata.core.Location;

/**
 * a simple object represent the query parameters
 * 
 * @author houdejun
 *
 */
public class PageQuery implements Cloneable{
	private int page = 1;
	private Date minTime;
	private Date maxTime;
	private boolean isLastPage = false;
	private String query;
	private Location location;
	public int getPage() {
		return page;
	}
	public void setPage(int page) {
		this.page = page;
	}
	public boolean isLastPage() {
		return isLastPage;
	}
	public void setLastPage(boolean isLastPage) {
		this.isLastPage = isLastPage;
	}
	public String getQuery() {
		return query;
	}
	public void setQuery(String query) {
		this.query = query;
	}
	public Date getMinTime() {
		return minTime;
	}
	public void setMinTime(Date minTime) {
		this.minTime = minTime;
	}
	public Date getMaxTime() {
		return maxTime;
	}
	public void setMaxTime(Date maxTime) {
		this.maxTime = maxTime;
	}
	public Location getLocation() {
		return location;
	}
	public void setLocation(Location location) {
		this.location = location;
	}
	public boolean isFirstPage(){
		if(this.page<=1){
			return true;
		}
		return false;
	}
	public PageQuery(String query){
		this.query = query;
		this.page = 1;
		isLastPage=false;
	}
	public PageQuery(String query,int page){
		this.query = query;
		this.page = page;
		isLastPage=false;
	}
	
	public PageQuery(String query,int page,Date minTime,Date maxTime){
		this.query = query;
		this.page = page;
		this.minTime = minTime;
		this.maxTime = maxTime;
		isLastPage=false;
	}
	
	public void nextPage(){
		this.page++;
	}
	
	@Override
	public String toString() {
		String str = this.query+" page "+this.page;
		if(this.minTime!=null){
			str += " minTime:"+DateTimeUtils.format(this.minTime, "yyyy-MM-dd HH:mm:ss");
		}
		if(this.maxTime!=null){
			str += " maxTime:"+DateTimeUtils.format(this.maxTime, "yyyy-MM-dd HH:mm:ss");
		}
		if(this.location!=null){
			str += " location:"+this.location.toString();
		}
		return  str;
	}
	
	@Override
	public PageQuery clone() {
		PageQuery place = new PageQuery(this.query,this.page);
		place.setMinTime(minTime);
		place.setMaxTime(maxTime);
		place.setLastPage(this.isLastPage);
		if(this.location!=null){
			place.setLocation((Location)this.location.clone());
		}
		return place;
	}
}