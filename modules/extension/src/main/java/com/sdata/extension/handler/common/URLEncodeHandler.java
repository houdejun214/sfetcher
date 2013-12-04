package com.sdata.extension.handler.common;

import java.net.URLDecoder;

import com.sdata.context.parser.IParserContext;
import com.sdata.extension.handler.IParserHandler;


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
