package com.sdata.core.parser.html.handler.dianping;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import com.sdata.core.parser.html.context.IParserContext;
import com.sdata.core.parser.html.handler.IParserHandler;

public class DianpingImageHandler implements IParserHandler {

	public Object handle(IParserContext context, Object data) {
		if(data == null) {
			return data;
		}else if(data instanceof String){
			return data.toString().replaceAll("249x249", "700x700");
		}else if(data instanceof List){
			List<String> imgs = new ArrayList<String>();
			Iterator iterator = ((List)data).iterator();
			while(iterator.hasNext()){
				Object str = iterator.next();
				imgs.add(str.toString().replaceAll("249x249", "700x700"));
			}
			return imgs;
		}
		return data;
		
	}

}
