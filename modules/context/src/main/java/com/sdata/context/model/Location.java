package com.sdata.context.model;

import java.math.BigDecimal;
import java.math.RoundingMode;

import com.lakeside.core.utils.StringUtils;

/**
 * object of geographic location information
 * 
 * @author houdj
 *
 */
public class Location implements Cloneable, Comparable<Location>{
	
	private BigDecimal longitude; //
	
	private BigDecimal latitude; // 
	
	private static final int Scale=6;

	public BigDecimal getLongitude() {
		return longitude;
	}

	public void setLongitude(BigDecimal longitude) {
		this.longitude = longitude;
	}

	public BigDecimal getLatitude() {
		return latitude;
	}

	public void setLatitude(BigDecimal latitude) {
		this.latitude = latitude;
	}
	
	public Location(){
		
	}

	public Location(Location loc){
		this.latitude = loc.getLatitude();
		this.longitude = loc.getLongitude();
		this.initScale();
	}
	
	private void initScale(){
		this.latitude=this.latitude.setScale(Scale,RoundingMode.HALF_UP);
		this.longitude=this.longitude.setScale(Scale,RoundingMode.HALF_UP);
	}
	
	public Location(BigDecimal lat , BigDecimal lon){
		this.latitude = lat;
		this.longitude = lon;
		this.initScale();
	}
	
	public Location(double lat , double lon){
		this.latitude = new BigDecimal(lat);
		this.longitude = new BigDecimal(lon);
		this.initScale();
	}
	public Location(String lat , String lon){
		if(lat!=null){
			this.latitude = new BigDecimal(lat.trim());
		}
		if(lon!=null){
			this.longitude = new BigDecimal(lon.trim());
		}
		this.initScale();
	}
	
	public Location(String location){
		if(StringUtils.isEmpty(location)){
			throw new IllegalArgumentException("location is empty");
		}
		String[] values = location.split(",");
		if(values==null || values.length!=2){
			throw new IllegalArgumentException("location is not a valid location string");
		}
		if(values[0]!=null){
			this.latitude = new BigDecimal(values[0].trim());
		}
		if(values[1]!=null){
			this.longitude = new BigDecimal(values[1].trim());
		}
		this.initScale();
	}
	
	public Location move(double latOffset , double longOffset){
		BigDecimal lat=this.latitude.add(new BigDecimal(latOffset));
		BigDecimal lng=this.longitude.add(new BigDecimal(longOffset));
		return new Location(lat,lng);
	}
	
	public Location move(BigDecimal latOffset , BigDecimal longOffset){
		BigDecimal lat=this.latitude.add(latOffset);
		BigDecimal lng=this.longitude.add(longOffset);
		return new Location(lat,lng);
	}
	

	@Override
	public boolean equals(Object obj) {
		if(!(obj instanceof Location)){
			return false;
		}
		Location other = (Location)obj;
		return this.latitude.compareTo(other.getLatitude())==0 && this.longitude.compareTo(other.getLongitude())==0;
	}

	@Override
	public String toString() {
		return this.latitude.toString() +","+ this.longitude.toString();
	}

	@Override
	public Object clone(){
		Location loc = new Location();
		loc.latitude = new BigDecimal(this.latitude.toString());
		loc.longitude = new BigDecimal(this.longitude.toString());
		return loc;
	}

	public int compareTo(Location o) {
		int compareTo = this.latitude.compareTo(o.getLatitude());
		if(compareTo==0){
			return this.longitude.compareTo(o.getLongitude());
		}
		return compareTo;
	}

	@Override
	public int hashCode() {
		return this.toString().hashCode();
	}
}
