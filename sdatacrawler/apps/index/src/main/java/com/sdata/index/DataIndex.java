package com.sdata.index;

import org.slf4j.bridge.SLF4JBridgeHandler;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import com.lakeside.core.utils.StringUtils;
import com.sdata.conf.ArgConfig;
import com.sdata.conf.IndexConfig;
import com.sdata.conf.sites.CrawlConfig;
import com.sdata.conf.sites.CrawlConfigManager;
import com.sdata.core.Configuration;
import com.sdata.core.Constants;
import com.sdata.core.CrawlAppContext;
import com.sdata.core.RunState;
import com.sdata.core.crawldb.CrawlDB;
import com.sdata.core.crawldb.impl.CrawlDBImpl;
import com.sdata.core.data.index.solr.IndexControler;

/**
 * DB DATA INDEX Implement
 * 
 * 
 * @author zhufb
 *
 */
public class DataIndex {
	private static final String ApplicationContextPath="applicationContext.xml";
	
	/**
	 * @param args
	 * @throws Exception 
	 */
	public static void main(String[] args) throws Exception {
		int argLength = args.length;
		ArgConfig argConfig = new ArgConfig(args, argLength);
		String indexName = argConfig.getValue("site","name");
		if(StringUtils.isEmpty(indexName)){
			System.out.println("site name is null ,please input indexed site name!");
			return;
		}
		// use slf4j replace jdk logging engine
		SLF4JBridgeHandler.install();
		//load application context;
		//start crawl webserver
		// load configuration
		CrawlConfigManager configs = CrawlConfigManager.load(indexName);
		CrawlConfig crawlSite = configs.getCurCrawlSite();
		//CrawlSite site = configs.getCrawlSite(indexName);
		crawlSite.putAllConf(argConfig);
		Configuration conf = crawlSite.getConf();
		CrawlAppContext.conf = conf;
		
		String siteName = conf.get(Constants.SOURCE);
		IndexControler.init(IndexConfig.getSite(siteName));
		//server
//		ApplicationContext context =CrawlAppContext.context=loadContext();
//		CrawlServer server = new CrawlServer();
//		server.start(conf,context);
		// create crawl database object
		CrawlDB db = getCrawlDB(conf);
		RunState state = new RunState(indexName,db);
		CrawlAppContext.db = db;
		CrawlAppContext.state = state;
		mainTask = new DBIndexTask();
		System.out.println("/****************** start to index ["+indexName+"] ******************/");
		mainTask.start();
	}
	
	private static ApplicationContext loadContext() throws Exception{
		ApplicationContext context = new ClassPathXmlApplicationContext(ApplicationContextPath);
		return context;
	}
	
	public static DBIndexTask mainTask;

	private static CrawlDB getCrawlDB(Configuration conf){
		CrawlDBImpl crawlDBImpl = new CrawlDBImpl(conf);
		return crawlDBImpl;
	}
}
