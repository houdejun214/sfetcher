package com.sdata.core.parser.html.notify;

import java.util.Map;


/**
 * @author zhufb
 * @param <T>
 *
 */
public abstract class CrawlNotify{
	
	public abstract void notify(Map<?, Object> data);
	
	protected void mail(String content){
	}
	
}