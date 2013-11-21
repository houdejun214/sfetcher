package com.sdata.index;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CountDownLatch;

import org.apache.commons.lang.StringUtils;

import com.sdata.core.Configuration;
import com.sdata.core.CrawlAppContext;
import com.sdata.core.data.FieldProcess;
import com.sdata.core.data.index.solr.IndexControler;

/**
 * @author zhufb
 * 
 */
public class DBIndexTask {
	private static boolean complete = false;
	private final static int THREADS = 25;
	private static List<Thread> processList = new ArrayList<Thread>();
	private static FieldProcess process ;
	private static CountDownLatch latch = new CountDownLatch(THREADS);
	public DBIndexTask() {
		Configuration config = CrawlAppContext.conf;
		String collection  = config.get("indexCollection");
		String host = config.get("mongoHost");
		int port = config.getInt("mongoPort",27017);
		String dbName = config.get("mongoDbName");
		DBDataDao dbData = new DBDataDao(collection);
		dbData.initilize(host, port, dbName);
		DBDataQueue queue = new DBDataQueue(dbData);
		process = new FieldProcess(config);
		FieldProcess processOther = null;
		String famousIndexPath = CrawlAppContext.conf.get("famousIndexPath");
		String topicIndexPath = CrawlAppContext.conf.get("topicIndexPath");
		if(!StringUtils.isEmpty(famousIndexPath)){
			processOther =  new FieldProcess(config,famousIndexPath);
		}else if(!StringUtils.isEmpty(topicIndexPath)){
			processOther = new FieldProcess(config,topicIndexPath);
		}
		for (int i = 0; i < THREADS; i++) {
			processList.add(new Thread(new DBIndexProcess(queue, process,processOther,dbData)));
		}
	}


	public static void start() throws InterruptedException {
		Iterator<Thread> iterator = processList.iterator();
		while (iterator.hasNext()) {
			iterator.next().start();
		}
		latch.await();
		//run state
		//process.completeImgSave();
		CrawlAppContext.state.saveIndexCount();
		IndexControler.complete();
		// solr
		System.out.println("DB data index task is finished!!!");
		return;
	}

	public static boolean isComplete() {
		return complete;
	}

	public static void setComplete(boolean complete) {
		DBIndexTask.complete = complete;
	}

	public static void countDown() {
		latch.countDown();
	}
}
