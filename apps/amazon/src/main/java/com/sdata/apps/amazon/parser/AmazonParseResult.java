package com.sdata.apps.amazon.parser;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.sdata.core.parser.ParseResult;

public class AmazonParseResult extends ParseResult {
	
	private List<Map<String,Object>> newCategoryList = new ArrayList<Map<String,Object>>();

	public List<Map<String, Object>> getNewCategoryList() {
		return newCategoryList;
	}

	public void setNewCategoryList(List<Map<String, Object>> newCategoryList) {
		this.newCategoryList = newCategoryList;
	}

    private boolean isBlock = false;

    public boolean isBlock() {
        return isBlock;
    }

    public void setBlock(boolean isBlock) {
        this.isBlock = isBlock;
    }

}
