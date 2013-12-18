package com.sdata.db;

import org.apache.commons.lang.StringUtils;


/**
 * @author zhufb
 *
 */
public class ColumnFamily {

	private String name;
	private String fields;
	private String[] fieldList;
	
	public ColumnFamily(String name,String fields){
		this.setName(name);
		this.setFields(fields);
	}
	
	
	public String getName() {
		return name;
	}
	public void setName(String name) {
		this.name = name;
	}
	public String getFields() {
		return fields;
	}
	
	public void setFields(String fields) {
		if(StringUtils.isEmpty(fields)){
			return;
		}
		this.fields = fields;
		this.fieldList = fields.split(",");
	}
	public String[] getFieldList() {
		return fieldList;
	}
}
