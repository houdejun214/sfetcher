package com.sdata.core.parser.html.handler;

import com.sdata.core.parser.html.context.IParserContext;



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
