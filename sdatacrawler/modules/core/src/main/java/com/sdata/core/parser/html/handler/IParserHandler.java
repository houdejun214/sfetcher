package com.sdata.core.parser.html.handler;

import com.sdata.core.parser.html.context.IParserContext;

public interface IParserHandler {
	public Object handle(IParserContext context, Object data);
}
