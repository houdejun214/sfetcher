package com.sdata.crawl.cluster;

import com.lakeside.core.StateEnum;


public class CrawlerState extends StateEnum {
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 605779624277379612L;

	protected CrawlerState(String name, int value) {
		super(name, value);
	}

	public static CrawlerState Activate = new CrawlerState("Activate",1); // 活跃状态
	 
	public static CrawlerState Stop = new CrawlerState("Stop", 2); // 程序正常关闭
	 
	public static CrawlerState Dead = new CrawlerState("Dead",3); // 程序非正常关闭或停止

 }