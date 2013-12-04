package com.sdata.core.index.es;

import org.apache.commons.lang.StringUtils;

import com.sdata.context.config.Constants;
import com.sdata.context.config.CrawlAppContext;
import com.sdata.elastic.Elastic;

/**
 * @author zhufb
 *
 */
public class ElasticServer {

	private static final Object syn = new Object();
	private static Elastic instance = null;

	public static Elastic getElastic(String clusterName){
		if(instance == null){
			synchronized(syn){
				if(instance == null){
					String sName = CrawlAppContext.conf.get(Constants.SOURCE);
					if(StringUtils.isEmpty(sName)){
						sName = CrawlAppContext.state.getCrawlName();
					}
					String mapping = CrawlAppContext.conf.get("ElasticMapping");
					if(StringUtils.isEmpty(sName)){
				    	throw new RuntimeException("get NSouce error,sourceName is null:"+sName);
					}
					instance =  new Elastic(clusterName,sName,mapping);
				}
			}
		}
		return instance;
	}
}
