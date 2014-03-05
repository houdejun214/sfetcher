package com.sdata.core.parser.html.field.datum;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.jsoup.nodes.Element;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.sdata.context.parser.IParserContext;
import com.sdata.core.parser.html.field.Field;


/**
 * @author zhufb
 *
 */
public class DatumItem extends DatumField{
	private static final Logger log = LoggerFactory.getLogger("SdataCrawler.DatumItem");
	public Object getData(IParserContext context,Element doc){
		if(!this.hasChild()){
			return refine(context,super.getSelectValue(context, doc));
		}
		
		Map<String,Object> result = new HashMap<String,Object>();
		List<Field> childs = this.getChilds();
		Iterator<Field> iterator = childs.iterator();
		while(iterator.hasNext()){
			Field f = iterator.next();
			Object name = f.getName();
			if(name == null||StringUtils.isEmpty(name.toString())){
				log.error("map parser exists no name field,plesae check it!");
				continue;
			}
			Object data = f.getData(context, doc);
			if(data!=null)
				result.put(name.toString(), data);
		}
		return refine(context,result);
	}
	
	public Object transData(IParserContext context,Map<String,Object> data){
		// item string not map
		if(!this.hasChild()){
			return refine(context,super.getInterData(context,data));
		}
		// item map
		Map<String,Object> result = new HashMap<String,Object>();
		List<Field> childs = getChilds();
		Iterator<Field> iterator = childs.iterator();
		while(iterator.hasNext()){
			DatumField f = (DatumField)iterator.next();
			Object v = f.transData(context,data);
			String name = f.getName();
			if(!StringUtils.isEmpty(name)&&v !=null){
				result.put(name, v);
			}
		}
		return refine(context,result.size() == 0?null:result);
	}
}