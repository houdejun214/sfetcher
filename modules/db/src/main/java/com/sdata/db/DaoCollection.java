package com.sdata.db;

import java.util.ArrayList;
import java.util.List;

/**
 * @author zhufb
 *
 */
public class DaoCollection {
	protected String source;
	protected String name;
	protected String primaryKey;
	protected String field;
	protected String index;
	protected String pkTable; 

	protected List<ColumnFamily> colflys = new ArrayList<ColumnFamily>(); 
	protected List<String> update = new ArrayList<String>(); 
	protected List<String> remove = new ArrayList<String>();

	public DaoCollection(String source){
		this.source = source;
	}
	
	public void addUpdate(String field) {
		update.add(field);
	}

	public void addRemove(String field) {
		remove.add(field);
	}

	public void addColumnFamily(ColumnFamily sc) {
		colflys.add(sc);
	}

	public String getName() {
		return name;
	}
	public void setName(String name) {
		this.name = name;
	}
	public String getPrimaryKey() {
		return primaryKey;
	}

	public void setPrimaryKey(String primaryKey) {
		this.primaryKey = primaryKey;
	}

	public List<String> getUpdate() {
		return update;
	}
	public void setUpdate(List<String> update) {
		this.update = update;
	}
	public List<String> getRemove() {
		return remove;
	}
	public void setRemove(List<String> remove) {
		this.remove = remove;
	}

	public String getField() {
		return field;
	}

	public String getPkTable() {
		return pkTable;
	}

	public void setPkTable(String pkTable) {
		this.pkTable = pkTable;
	}

	public void setField(String field) {
		this.field = field;
	}

	public String getIndex() {
		return index;
	}

	public void setIndex(String index) {
		this.index = index;
	}

	public List<ColumnFamily> getColflys() {
		return colflys;
	}

	public void setColflys(List<ColumnFamily> colflys) {
		this.colflys = colflys;
	}

	public String getSource() {
		return source;
	}

	public void setSource(String source) {
		this.source = source;
	}

}
