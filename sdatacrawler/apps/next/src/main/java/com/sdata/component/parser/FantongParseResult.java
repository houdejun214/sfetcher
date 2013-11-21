package com.sdata.component.parser;


import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.sdata.core.parser.ParseResult;

public class FantongParseResult extends ParseResult {
	
	private List<Map<String,Object>> newCategoryList = new ArrayList<Map<String,Object>>();

	public List<Map<String, Object>> getNewCategoryList() {
		return newCategoryList;
	}

	public void setNewCategoryList(List<Map<String, Object>> newCategoryList) {
		this.newCategoryList = newCategoryList;
	}
	
}
