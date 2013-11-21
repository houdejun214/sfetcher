package com.sdata.core.parser.html.field;


import org.jsoup.nodes.Element;

import com.sdata.core.parser.html.context.IParserContext;

public interface Field {
	
	public Object getData(IParserContext context,Element doc);
	
	public void addChildField(Field field);
	
	public boolean hasChild();
	
	public String getName();
}
