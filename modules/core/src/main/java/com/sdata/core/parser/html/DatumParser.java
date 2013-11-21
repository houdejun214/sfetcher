package com.sdata.core.parser.html;

import java.util.Iterator;
import java.util.Map;

import org.jsoup.nodes.Document;

import com.sdata.core.Configuration;
import com.sdata.core.parser.html.config.DatumConfig;
import com.sdata.core.parser.html.context.DatumContext;
import com.sdata.core.parser.html.field.Field;

/**
 * @author zhufb
 *
 */
public class DatumParser{
	private DatumContext context;
	public DatumParser(Configuration conf,Document doc){
		context = new DatumContext(conf,doc);
	}

	public DatumParser(DatumContext context){
		this.context = context;
	}
	
	public void addContext(String key,Object value ){
		this.context.putVariable(key, value);
	}
	
	public Map<String,Object> analysis(){
		Iterator<Field> fields = DatumConfig.getInstance(context.getConfig()).getFields();
		while(fields.hasNext()){
			Field field = fields.next();
			String name = field.getName();
			if(!context.containData(name)){
				Object data = field.getData(context, context.getDoc());
				if(data!=null&&!"".equals(data))
					context.addData(name,data);
			}
		}
		return context.getMetadata();
	}
}
