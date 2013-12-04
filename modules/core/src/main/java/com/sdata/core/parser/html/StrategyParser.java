package com.sdata.core.parser.html;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.sdata.core.parser.config.StrategyConfig;
import com.sdata.core.parser.html.context.StrategyContext;
import com.sdata.core.parser.html.field.Tags;
import com.sdata.core.parser.html.notify.CrawlNotify;

/**
 * @author zhufb
 * 
 *
 */
public abstract class StrategyParser{
	
	protected StrategyContext context;
	
	public void addContext(String key,Object value ){
		this.context.putVariable(key, value);
	}
	
	public Map<Tags,Object> analysis(){
		Map<Tags,Object> result = new HashMap<Tags,Object>();
		// 获取搜有的link和datum标签
		StrategyConfig instance = StrategyConfig.getInstance(context.getConfig());
		if(instance == null){
			throw new RuntimeException("not found strategy file!");
		}
		result.put(Tags.LINKS, getData(Tags.LINKS));
		result.put(Tags.DATUM, getData(Tags.DATUM));
		CrawlNotify crawlNotify = instance.getCrawlNotify();
		crawlNotify.notify(result);
		return result;
	}
	
	protected abstract List<Object> getData(Tags tag);

}
