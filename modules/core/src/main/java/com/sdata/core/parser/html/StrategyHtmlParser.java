package com.sdata.core.parser.html;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.jsoup.nodes.Document;

import com.sdata.context.config.Configuration;
import com.sdata.core.parser.config.StrategyConfig;
import com.sdata.core.parser.html.context.StrategyContext;
import com.sdata.core.parser.html.field.Field;
import com.sdata.core.parser.html.field.Tags;

/**
 * @author zhufb
 * 
 *
 */
public class StrategyHtmlParser extends StrategyParser{

    public StrategyHtmlParser(Configuration conf,Document doc){
		context = new StrategyContext(conf,doc);
	}

    public StrategyContext getContext() {
        return this.context;
    }

    protected List<Object> getData(Tags tag){
		List<Object> result = new ArrayList<Object>();
		List<Field> list = StrategyConfig.getInstance(context.getConfig()).getTag(tag.getName());
		Iterator<Field> iterator = list.iterator();
		while(iterator.hasNext()){
			Field field = iterator.next();
			Object o = field.getData(context, context.getDoc());
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
