package com.sdata.core.data.index.solr;

import java.util.List;

/**
 * @author zhufb
 *
 */
public class IndexSite {
	private String name;
	private List<IndexSource> list;
	public String getName() {
		return name;
	}
	public void setName(String name) {
		this.name = name;
	}
	public List<IndexSource> getList() {
		return list;
	}
	public void setList(List<IndexSource> list) {
		this.list = list;
	}
}
