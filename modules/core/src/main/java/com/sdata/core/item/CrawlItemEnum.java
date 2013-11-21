package com.sdata.core.item;

import org.apache.commons.lang.enums.ValuedEnum;

/**
 * @author zhufb
 *
 */
public class CrawlItemEnum extends ValuedEnum {

	private static final long serialVersionUID = 4260350618206782775L;
	protected CrawlItemEnum(String name, int value) {
		super(name, value);
	}

	public static CrawlItemEnum KEYWORD = new CrawlItemEnum("keywords",1);
	public static CrawlItemEnum ACCOUNT = new CrawlItemEnum("account",2);
	public static CrawlItemEnum LOCATION = new CrawlItemEnum("location",3);
	public static CrawlItemEnum TIMERANGE = new CrawlItemEnum("timeRange",4);

	public static CrawlItemEnum getEnum(int value){
		return (CrawlItemEnum)getEnum(CrawlItemEnum.class,value);
	}

	public static String getEnumName(int value){
		return  getEnum(value).getName();
	}
}
