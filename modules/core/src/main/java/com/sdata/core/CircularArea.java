package com.sdata.core;

import java.math.BigDecimal;

public class CircularArea {
	
	protected Location loc;
	
	protected int distance = 1000;
	
	protected BigDecimal areaSpan;
	
	public BigDecimal getMinX(){
		return loc.getLongitude().subtract(new BigDecimal(Double.toString((this.distance*0.707)/111133)));
	}
	
	public BigDecimal getMinY(){
		return loc.getLatitude().subtract(new BigDecimal(Double.toString((this.distance*0.707)/111133)));
	}
	
	public BigDecimal getMaxX(){
		return loc.getLongitude().add(new BigDecimal(Double.toString((this.distance*0.707)/111133)));
	}
	
	public BigDecimal getMaxY(){
		return loc.getLatitude().add(new BigDecimal(Double.toString((this.distance*0.707)/111133)));
	}
	
	public Location getLoc() {
		return loc;
	}
	public void setLoc(Location loc) {
		this.loc = loc;
	}
	public int getDistance() {
		return distance;
	}
	public void setDistance(int distance) {
		this.distance = distance;
	}
	
	public CircularArea(){
		loc = new Location();
	}
	
	public CircularArea(Location loc,BigDecimal latOffset,BigDecimal longOffset,int distance){
		this.loc=loc;
		this.distance = distance;
//		this.rt = loc.move(latOffset, longOffset);
	}
	
	public void move(BigDecimal latOffset,BigDecimal longOffset){
		this.loc = this.loc.move(latOffset, longOffset);
	}
	
}
