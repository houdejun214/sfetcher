package com.sdata.core.parser.html.field;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.enums.Enum;

/**
 * tag type contains field,list,item
 * 
 * @author zhufb
 *
 */
public class Tags extends Enum{
	
	private static final long serialVersionUID = 5874959143119367592L;
	//Page
	public static final Tags FIELD = new Tags("field");
	public static final Tags LIST = new Tags("list");
	public static final Tags ITEM = new Tags("item");
	//Strategy
	public static final Tags INIT = new Tags("init");
	public static final Tags LINKS = new Tags("links");
	public static final Tags DATUM = new Tags("datum");
	public static final Tags DATA = new Tags("data");
	
	public Tags(String type) {
		super(type);
	}

    public static Tags getEnum(String type) {
      return (Tags) getEnum(Tags.class, type);
    }

    public static Map<?, ?> getEnumMap() {
      return getEnumMap(Tags.class);
    }

    public static List<?> getEnumList() {
      return getEnumList(Tags.class);
    }

    public static Iterator<?> iterator() {
      return iterator(Tags.class);
    }
}
