package com.sdata.core.parser.select;

import java.util.regex.Matcher;



public class DataSelectorFormat extends DataSelector {
	
	
	private SelectorFormattor formatter;
	
	public DataSelectorFormat(String selector){
		this.selector = selector;
		Matcher matcher = SELECTOR_FORMAT.matcher(selector);
		if(matcher.find()){
			String _format = matcher.group(1);
			formatter = new SelectorFormattor(_format);
		}
	}
	
	@Override Object selectObject(Object inputObject){
		return formatter.format(inputObject, context);
	}
}
