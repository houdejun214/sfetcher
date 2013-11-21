package com.sdata.sense.handler;

import org.apache.commons.lang.StringUtils;

import com.lakeside.core.utils.PatternUtils;
import com.sdata.core.parser.html.context.IParserContext;
import com.sdata.core.parser.html.handler.IParserHandler;

public class HtmlPageHandler implements IParserHandler {

	public Object handle(IParserContext context, Object data) {
		if(data == null){
			return null;
		}
		String result = null;
		String url = data.toString();
		String t = PatternUtils.getMatchPattern("&p=(\\d*)&?", url, 1);
		if(StringUtils.isEmpty(t)){
			result = url.concat("&p=2");
		}else{
			int p = Integer.parseInt(t)+1;
			result = url.replace("&p="+t, "&p="+p);
		}
		return result;
	}

}
