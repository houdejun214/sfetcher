package com.sdata.sense.fetcher.statistic;

import java.lang.reflect.Method;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.sdata.core.Configuration;
import com.sdata.core.FetchDispatch;
import com.sdata.core.RunState;
import com.sdata.sense.SenseConfig;
import com.sdata.sense.SenseFetchDatum;
import com.sdata.sense.fetcher.SenseFetcher;
import com.sdata.sense.item.SenseCrawlItem;

/**
 * @author zhufb
 *
 */
public class SenseStatisticFetcher extends SenseFetcher{
	protected static  Logger log = LoggerFactory.getLogger("Sense.SenseStatisticFetcher");
	public final static String FID = "statistic";
	
	public SenseStatisticFetcher(Configuration conf,RunState state){
		super(conf,state);
	}
	
	@Override
	public void fetchDatumList(FetchDispatch fetchDispatch, SenseCrawlItem crawlItem) {
		Configuration conf = SenseConfig.getConfig(crawlItem);
		log.warn("fetch statistic:"+crawlItem.parse());
		String cls = conf.get("sense.statistic.bean");
		this.invoke(cls, fetchDispatch,conf, crawlItem);
	}
	
	protected void  invoke(String className,FetchDispatch fetchDispatch,Configuration conf,SenseCrawlItem crawlItem) {
		try{
			Class<?> classType = Class.forName(className);
			Object statistic = classType.newInstance();
			Method method = classType.getMethod("statistic", new Class[]{FetchDispatch.class,Configuration.class,SenseCrawlItem.class});
			method.invoke(statistic,fetchDispatch, conf,crawlItem);
		}catch(Exception e){
			throw new RuntimeException(e);
		}
	}
	
	@Override
	public SenseFetchDatum fetchDatum(SenseFetchDatum datum){
		return datum;
	}
	
}
