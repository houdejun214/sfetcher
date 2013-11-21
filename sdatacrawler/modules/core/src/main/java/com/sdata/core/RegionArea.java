package com.sdata.core;

import java.math.BigDecimal;

public class RegionArea {
	
	protected Location lb;
	
	protected Location rt;
	
	public Location getLb() {
		return lb;
	}
	public void setLb(Location lb) {
		this.lb = lb;
	}
	public Location getRt() {
		return rt;
	}
	public void setRt(Location rt) {
		this.rt = rt;
	}
	
	public RegionArea(){
		lb = new Location();
		rt = new Location();
	}
	
	public BigDecimal getMinX(){
		return lb.getLongitude();
	}
	
	public BigDecimal getMinY(){
		return lb.getLatitude();
	}
	
	public BigDecimal getMaxY(){
		return rt.getLatitude();
	}
	
	public BigDecimal getMaxX(){
		return rt.getLongitude();
	}
	
	public RegionArea(Location loc,BigDecimal latOffset,BigDecimal longOffset){
		this.lb=loc;
		this.rt = loc.move(latOffset, longOffset);
	}
	
	public void move(BigDecimal latOffset,BigDecimal longOffset){
		this.lb = this.lb.move(latOffset, longOffset);
		this.rt = this.rt.move(latOffset, longOffset);
	}
	
	@Override
	public String toString() {
		return this.lb.toString()+" - " +this.rt.toString();
	}
}
