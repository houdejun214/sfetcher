package com.sdata.common;

import java.util.Map;

import com.lakeside.core.utils.StringUtils;
import com.sdata.proxy.item.SenseCrawlItem;

/**
 * 
 * Crawl item for crawler from SCMS
 * 
 * @author zhufb
 *
 */
public class CommonItem extends SenseCrawlItem {
	
	public CommonItem(Map<String,Object> map){
		super(map);
		if(map == null){
			return;
		}
		this.urlPattern =StringUtils.valueOf(getParam(CommonParamEnum.URL_PATTERN.getName()));
	}
	
	private String urlPattern;
	
	@Override
	public String parse(){
		return this.entryUrl;
	}
	@Override
	public String toString(){
		return CommonItem.class.getName();
	}
	
	public String getUrlPattern() {
		return urlPattern;
	}
}
