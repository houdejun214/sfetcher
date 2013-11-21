package com.sdata.core.parser.html;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import net.sf.json.JSONObject;

import org.apache.commons.lang.StringUtils;

import com.sdata.core.Configuration;
import com.sdata.core.parser.html.config.StrategyConfig;
import com.sdata.core.parser.html.context.StrategyContext;
import com.sdata.core.parser.html.field.Field;
import com.sdata.core.parser.html.field.StrategyField;
import com.sdata.core.parser.html.field.Tags;

/**
 * @author zhufb
 * 
 *
 */
public class StrategyJSONParser extends StrategyParser{
	private JSONObject json;
	public StrategyJSONParser(Configuration conf,JSONObject json){
		this.context = new StrategyContext(conf,json);
		this.json = json;
	}
	
	protected List<Object> getData(Tags tag){
		List<Object> result = new ArrayList<Object>();
		List<Field> list = StrategyConfig.getInstance(context.getConfig()).getTag(tag.getName());
		Iterator<Field> iterator = list.iterator();
		while(iterator.hasNext()){
			StrategyField field = (StrategyField)iterator.next();
			Object o = field.getData(context, json);
			if(o == null||StringUtils.isEmpty(o.toString())){
				continue;
			}else if(o instanceof List){
				result.addAll((List)o);
			}else{
				result.add(o);
			}
		}
		return result;
	}

}
