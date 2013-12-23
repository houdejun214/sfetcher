package com.sdata.common;

import java.net.MalformedURLException;
import java.util.List;
import java.util.Map;

import com.lakeside.core.utils.StringUtils;
import com.lakeside.core.utils.UrlUtils;
import com.sdata.context.config.Configuration;
import com.sdata.proxy.SenseConfig;
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
		Object patterns = getParam(CommonParamEnum.URL_PATTERN.getName());
		StringBuffer sb = new StringBuffer();
		if(patterns instanceof List){
			List list = (List)patterns;
			for(Object str:list){
				sb.append("|").append(str);
			}
			this.urlPattern = sb.substring(1);
		}else{
			this.urlPattern = StringUtils.valueOf(patterns);
		}
		
		try {
			this.domain = UrlUtils.getDomainName(this.entryUrl);
		} catch (MalformedURLException e) {
			throw new RuntimeException(e);
		}
		String name = CommonParamEnum.LEVEL_LIMIT.getName();
		if(super.containParam(name)){
			this.levelLimit =(Integer)getParam(name);
		}
		if(levelLimit == null||levelLimit == 0){
			Configuration config = SenseConfig.getConfig(this);
			this.levelLimit = config.getInt("crawler.level.limit", 3);
		}
	}
	
	private String domain;
	private String urlPattern;
	private Integer levelLimit;
	
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
	
	public Integer getLevelLimit() {
		return levelLimit;
	}
	public String getDomain() {
		return domain;
	}
}
