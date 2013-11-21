package com.sdata.crawl;


import org.apache.commons.lang.StringUtils;
import org.slf4j.bridge.SLF4JBridgeHandler;

import com.sdata.conf.ArgConfig;
import com.sdata.conf.sites.CrawlConfig;
import com.sdata.conf.sites.CrawlConfigManager;
import com.sdata.core.Configuration;
import com.sdata.core.CrawlAppContext;
import com.sdata.core.RunState;
import com.sdata.core.crawldb.CrawlDB;
import com.sdata.core.crawldb.impl.CrawlDBImpl;
import com.sdata.core.http.proxy.ProxyConfig;
import com.sdata.core.parser.html.mail.CrawlerMail;
import com.sdata.crawl.task.CrawlTask;

public class DataCrawl {
	
	private static final String DEFAULT_CRAWL_NAME = "test";
	private static final String ApplicationContextPath="applicationContext.xml";
	
	/**
	 * @param args
	 * @throws Exception 
	 */
	public static void main(String[] args) throws Exception {
		
		int argLength = args.length;
		ArgConfig argConfig = new ArgConfig(args, argLength);
		// the crawler name can be site or name;
		String crawlName = argConfig.getValue("crawl","crawlName","name");//crawl
		if(StringUtils.isEmpty(crawlName)){
			
			crawlName = DEFAULT_CRAWL_NAME;
		}
		String site= argConfig.getValue("site","siteName");//site
		boolean isTemplate = argConfig.haveArg("tpl");//tpl
		//crawlName = "weiboSense";
		// use slf4j replace jdk logging engine
		SLF4JBridgeHandler.install();
		// load configuration
		CrawlConfigManager configs = CrawlConfigManager.load(crawlName,site,isTemplate);
		Configuration conf = configs.getDefaultConf();
		CrawlConfig crawlSite = configs.getCurCrawlSite();
		if(crawlSite!=null){
			crawlSite.putAllConf(argConfig);
			conf = crawlSite.getConf();
		}else{
			conf.putAll(argConfig);
		}
		CrawlAppContext.conf = conf;
		// create crawl database object
		CrawlDB db = getCrawlDB(conf);
		CrawlAppContext.db = db;
		
		//start crawl solr server controler
//		String siteName = conf.get(Constants.SOURCE);
//		IndexControler.init(IndexConfig.getSite(siteName));
		
		// load proxy list
		if("true".equals(conf.get("UseProxy"))){
			ProxyConfig.loadAndSetProxy(configs.getDefaultConf("ProxyFile"));
		}
		// set network address cache time to live forever
		java.security.Security.setProperty("networkaddress.cache.ttl", "-1");
		RunState state = new RunState(crawlName,db);
		CrawlAppContext.state = state;
		mainTask = new CrawlTask(crawlSite,state);
		System.out.println("/****************** start to crawl ["+crawlName+"] ******************/");
//		CrawlClusterManager cluster = new CrawlClusterManager(mainTask,conf);
//		cluster.startCrawler();
		mainTask.startCrawl();
		CrawlerMail.send();
	}

	public static CrawlTask mainTask;
	
	private static CrawlDB getCrawlDB(Configuration conf){
		CrawlDBImpl crawlDBImpl = new CrawlDBImpl(conf);
		return crawlDBImpl;
	}
}
