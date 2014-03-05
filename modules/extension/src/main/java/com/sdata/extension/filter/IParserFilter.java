package com.sdata.extension.filter;

import com.sdata.context.parser.IParserContext;

public interface IParserFilter {
	
	public boolean filter(IParserContext context, Object data);
}
