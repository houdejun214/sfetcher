package com.sdata.core;

import java.math.BigDecimal;
import java.util.Calendar;
import java.util.Date;

import com.lakeside.core.utils.StringUtils;
import com.lakeside.core.utils.time.DateTimeUtils;

public class TimeSpanCircularArea extends CircularArea implements Cloneable{
	public static final String SEPARATOR = "  ";
	private Date startTime;
	private int timeSpan;
	
	public Date getStartTime() {
		return startTime;
	}

	public void setStartTime(Date startTime) {
		this.startTime = startTime;
	}

	public int getTimeSpan() {
		return timeSpan;
	}

	public void setTimeSpan(int timeSpan) {
		this.timeSpan = timeSpan;
	}

	public TimeSpanCircularArea(Location loc,BigDecimal latOffset,BigDecimal longOffset,int distance,Date startTime,int timeSpan){
		super(loc,latOffset,longOffset,distance);
		this.startTime = startTime;
		this.timeSpan = timeSpan;
	}
	
	public void moveNextTime(){
		this.startTime = DateTimeUtils.add(this.startTime, Calendar.SECOND, timeSpan);
	}

	@Override
	public void move(BigDecimal latOffset, BigDecimal longOffset) {
		super.move(latOffset, longOffset);
	}

	public TimeSpanCircularArea(){
		
	}
	public TimeSpanCircularArea clone(){
		TimeSpanCircularArea area= new TimeSpanCircularArea();
		area.loc = (Location)this.loc.clone();
		area.distance = this.distance;
		area.startTime = this.startTime;
		area.timeSpan = this.timeSpan;
		return area;
	}
	
	public static TimeSpanCircularArea convert(String area){
		String str_value=area;
		if(StringUtils.isEmpty(str_value)){
			return null;
		}
		TimeSpanCircularArea timeSpanArea = new TimeSpanCircularArea();
		String[] pattern = area.split(SEPARATOR);
		if(pattern.length!=0){
			String [] loc = pattern[0].split(",");
			timeSpanArea.setLoc(new Location(loc[0],loc[1]));
			timeSpanArea.setDistance(Integer.valueOf(pattern[1]));
			timeSpanArea.setStartTime(DateTimeUtils.parse(pattern[2], "yyyy-MM-dd HH:mm:ss"));
			timeSpanArea.setTimeSpan(Integer.valueOf(pattern[3]));
			return timeSpanArea;
		}
		return timeSpanArea;
	}
	@Override
	public String toString() {
		return this.loc.toString() + SEPARATOR + distance + SEPARATOR + DateTimeUtils.format(startTime, "yyyy-MM-dd HH:mm:ss") + SEPARATOR + timeSpan;
	}
}
