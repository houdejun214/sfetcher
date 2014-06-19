package com.sdata.core.parser;

import java.math.BigDecimal;
import java.util.*;

import com.lakeside.core.utils.StringUtils;
import com.sdata.core.FetchDatum;

public class ParseResult {
	
	private List<FetchDatum> fetchList = new ArrayList<FetchDatum>();
	private List<Map<String,Object>> categoryList = new ArrayList();
	private String nextUrl;
	private Map metadata = new HashMap<String,Object>(); 
	
	public void addFetchDatum(FetchDatum datum){
		this.fetchList.add(datum);
	}
	

	public List<Map<String,Object>> getCategoryList() {
		return categoryList;
	}

	public List<FetchDatum> getFetchList() {
		return fetchList;
	}

	public void setFetchList(List<FetchDatum> fetchList) {
		this.fetchList = fetchList;
	}

	public void setCategoryList(List categoryList) {
		this.categoryList = categoryList;
	}

	public void addCategory(Map<String,Object> cate) {
		this.categoryList.add(cate);
	}

	public Map getMetadata() {
		return metadata;
	}

    private boolean isBlock = false;

    public boolean isBlock() {
        return isBlock;
    }

    public void setBlock(boolean isBlock) {
        this.isBlock = isBlock;
    }
	
	public boolean isListEmpty(){
		if(fetchList==null || fetchList.size()==0){
			return true;
		}
		return false;
	}
	
	public int getListSize(){
		if(fetchList!=null ){
			return fetchList.size();
		}
		return 0;
	}

	public void setMetadata(Map metadata) {
		this.metadata.clear();
		this.metadata.putAll(metadata);
	}
	
	public void addAllMeta(Map metadata) {
		if(this.metadata==null){
			this.metadata = new HashMap<Object,Object>();
		}
		this.metadata.putAll(metadata);
	}

	public String getNextUrl() {
		return nextUrl;
	}

	public void setNextUrl(String nextUrl) {
		this.nextUrl = nextUrl;
	}
	
	public Long getLong(String key){
		String value = this.getString(key);
		if(value==null || !StringUtils.isNum(value)){
			return null;
		}
		return Long.valueOf(value);
	}
	
	public Integer getInt(String key,int defaultValue){
		String value = this.getString(key);
		if(value==null || !StringUtils.isNum(value)){
			return defaultValue;
		}
		return Integer.valueOf(value);
	}
	
	public BigDecimal getBigDecimal(String key){
		String value = this.getString(key);
		if(value==null){
			return null;
		}
		return new BigDecimal(value);
	}
	
	public String getString(String key){
		if(this.metadata!=null){
			return StringUtils.valueOf(this.metadata.get(key));
		}
		return null;
	}
}
