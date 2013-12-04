package com.sdata.context.model;

import java.math.BigDecimal;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.lakeside.core.utils.StringUtils;

public class PageRegionArea extends RegionArea implements Cloneable{
	private int page = 1;
	private boolean isLastPage;
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

	public PageRegionArea(){
		
	}
	public PageRegionArea(Location loc,BigDecimal latOffset,BigDecimal longOffset){
		super(loc,latOffset,longOffset);
	}
	
	public void moveNextPage(){
		page++;
	}

	@Override
	public String toString() {
		return super.toString() + " - page "+page;
	}

	@Override
	public void move(BigDecimal latOffset, BigDecimal longOffset) {
		super.move(latOffset, longOffset);
		this.page=1;
		this.isLastPage=false;
	}

	public PageRegionArea clone(){
		PageRegionArea area= new PageRegionArea();
		area.page=this.page;
		area.lb = (Location)this.lb.clone();
		area.rt = (Location)this.rt.clone();
		return area;
	}
	
	public static PageRegionArea convert(String area){
			String str_value=area;
			if(StringUtils.isEmpty(str_value)){
				return null;
			}
			PageRegionArea pageArea = new PageRegionArea();
			String pattern = "([-+]?[0-9]*\\.?[0-9]+),([-+]?[0-9]*\\.?[0-9]+) - ([-+]?[0-9]*\\.?[0-9]+),([-+]?[0-9]*\\.?[0-9]+) - page ([0-9]*)";
			Pattern compile = Pattern.compile(pattern);
			Matcher matcher = compile.matcher(str_value.trim());
			if(matcher.matches()){
				pageArea.setLb(new Location(matcher.group(1),matcher.group(2)));
				pageArea.setRt(new Location(matcher.group(3),matcher.group(4)));
				int page = Integer.valueOf(matcher.group(5).trim());
				pageArea.setPage(page);
				return pageArea;
			}
			//1.216332,103.633058 - 1.217156,103.633995, page 1
			String[] values = str_value.split("-");
			if(values==null || values.length!=3){
				return null;
			}
			String[] lbValues = values[0].split(",");
			pageArea.setLb(new Location(lbValues[0],lbValues[1]));
			String[] rtValues = values[1].split(",");
			pageArea.setRt(new Location(rtValues[0],rtValues[1]));
			String str_page = values[2];
			int page = Integer.valueOf(str_page.trim().split(" ")[1]);
			pageArea.setPage(page);
			return pageArea;
	}
}
