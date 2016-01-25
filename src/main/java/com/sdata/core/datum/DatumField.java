package com.sdata.core.datum;

import com.lakeside.core.utils.StringUtils;
import com.sdata.core.parser.select.DataSelector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 *
 */
public class DatumField extends Field{
	private static final Logger log = LoggerFactory.getLogger("SdataCrawler.DatumField");
	protected String name;
	protected String type;
	protected String from;
	
	protected String next;
	protected DataSelector nextSelector;

	private Map<Object,String> pageMap = new HashMap<Object,String>();
	
	protected String[] fromFields;
	protected String index;
	protected String indexOnly;
	protected DataSelector dataSelector;
	protected List<Field> subFields = new ArrayList<Field>(); 

	public void addChildField(Field field){
		this.subFields.add(field);
	}
	
	public void setChilds(List<Field> subFields) {
		this.subFields = subFields;
	}

	public List<Field> getChilds(){
		return this.subFields;
	}
	
	public boolean hasChild(){
		return this.subFields.size()>0?true:false;
	}
	
	public int getChildsSize(){
		return this.subFields.size();
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getType() {
		return type;
	}

	public void setType(String type) {
		this.type = type;
	}

	public String getFrom() {
		return from;
	}

	public void setFrom(String from) {
		this.from = from;
		if(!StringUtils.isEmpty(from)){
			this.fromFields = from.split(",");
		}
	}

	public String getIndex() {
		return index;
	}

	public void setIndex(String index) {
		this.index = index;
	}

	public String getIndexOnly() {
		return indexOnly;
	}

	public void setIndexOnly(String indexOnly) {
		this.indexOnly = indexOnly;
	}

	public String getNext() {
		return next;
	}
}