package com.sdata.core.parser.html;

import com.google.common.collect.Lists;
import com.sdata.core.parser.config.StrategyConfig;
import com.sdata.core.parser.html.context.StrategyContext;
import com.sdata.core.parser.html.field.Tags;

import java.util.*;

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

    public void addAllContext(Map<String, Object> maps) {
        this.context.putVariableAll(maps);
    }
	
	public Map<Tags,Object> analysis(){
		Map<Tags,Object> result = new HashMap<Tags,Object>();
		// 获取搜有的link和datum标签
		StrategyConfig instance = StrategyConfig.getInstance(context.getConfig());
		if(instance == null){
			throw new RuntimeException("not found strategy file!");
		}
        mergeList(result,Tags.LINKS, getData(Tags.LINKS));
        mergeList(result,Tags.LINKS, getData(Tags.ITORTOR));
        mergeList(result,Tags.DATUM, getData(Tags.DATUM));
		return result;
	}

    private void mergeList(Map<Tags,Object> result, Tags tag, List<Object> list) {
        Object o = result.get(tag);
        if (o == null) {
            result.put(tag, list);
        }else if (o instanceof List) {
            List old = (List) o;
            old.addAll(list);
        }else {
            ArrayList<Object> newlist = Lists.newArrayList(o);
            newlist.addAll(list);
            result.put(tag, newlist);
        }
    }

    protected abstract List<Object> getData(Tags tag);

}
