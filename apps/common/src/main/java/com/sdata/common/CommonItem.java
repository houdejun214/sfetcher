package com.sdata.common;

import java.util.List;
import java.util.Map;

import com.lakeside.core.utils.MapUtils;
import com.lakeside.core.utils.StringUtils;
import com.lakeside.core.utils.UrlUtils;
import com.sdata.context.config.Configuration;
import com.sdata.proxy.Constants;
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
		this.typeName = MapUtils.getString(map,"type_name");
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
		
		this.host = UrlUtils.getHost(this.entryUrl);
		String name = CommonParamEnum.LEVEL_LIMIT.getName();
		if(super.containParam(name)){
			this.levelLimit =Integer.valueOf(getParam(name).toString());
		}
		if(levelLimit == null||levelLimit == 0){
			Configuration config = SenseConfig.getConfig(this);
			this.levelLimit = config.getInt("crawler.level.limit", 3);
		}
	}
	
	private String host;
	private String urlPattern;
	private Integer levelLimit;
	private String typeName;
	
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
	
	public String getHost() {
		return host;
	}
	public String getTypeName() {
		return typeName;
	}
	public Map<String,Object> toMap() {
		Map<String,Object> result  = super.toMap();
		result.put(Constants.DATA_TAGS_FROM_TYPE,this.typeName);
		return result;
	}
}
