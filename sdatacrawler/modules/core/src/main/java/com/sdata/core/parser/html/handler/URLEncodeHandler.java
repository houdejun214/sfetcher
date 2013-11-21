package com.sdata.core.parser.html.handler;

import java.net.URLDecoder;

import com.sdata.core.parser.html.context.IParserContext;


/**
 * @author zhufb
 *
 */
public class URLEncodeHandler implements IParserHandler {
	public Object handle(IParserContext context, Object data) {
		if(data == null) {
			return data;
		}else{
			try {
				data = URLDecoder.decode(data.toString(), "UTF-8");
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		return data;
		
	}

}
