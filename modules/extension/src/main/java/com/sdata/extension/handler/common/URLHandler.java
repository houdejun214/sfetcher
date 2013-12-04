package com.sdata.extension.handler.common;

import com.sdata.context.parser.IParserContext;
import com.sdata.extension.handler.IParserHandler;



/**
 * @author zhufb
 *
 */
public class URLHandler implements IParserHandler {
	public Object handle(IParserContext context, Object data) {
		if(data == null) {
			return data;
		}
		return data.toString().replaceAll(" ", "%20");
	}

}
