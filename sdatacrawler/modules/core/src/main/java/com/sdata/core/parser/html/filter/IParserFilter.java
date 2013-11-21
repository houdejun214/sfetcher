package com.sdata.core.parser.html.filter;

import com.sdata.core.parser.html.context.IParserContext;

public interface IParserFilter {
	
	public boolean filter(IParserContext context, Object data);
}
