package com.sdata.core.parser.select;

import java.util.regex.Matcher;

import com.lakeside.core.utils.StringUtils;



public class DataSelectorArrays extends DataSelector {
	
	private int index = 0;
	
	public DataSelectorArrays(String selector){
		this.selector = selector;
		Matcher matcher = SELECTOR_ARRAYS.matcher(selector);
		if(matcher.find()){
			String group = matcher.group(1);
			index = StringUtils.toInt(group);
		}
	}
	
	@Override Object selectObject(Object inputObject){
		return this.getIndexAtObjectIfList(inputObject, index);
	}
}
