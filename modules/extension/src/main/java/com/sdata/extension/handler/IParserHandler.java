package com.sdata.extension.handler;

import com.sdata.context.parser.IParserContext;


/**
 * @author zhufb
 *
 */
public interface IParserHandler {
	
	public Object handle(IParserContext context, Object data);
}
