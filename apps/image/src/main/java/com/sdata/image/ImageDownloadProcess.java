package com.sdata.image;

import com.sdata.conf.sites.CrawlConfigManager;
import com.sdata.core.Configuration;

public class ImageDownloadProcess {
	private final static String  imageConfXml = "crawl-image.xml";

	public static void main(String []args){
		Configuration config = CrawlConfigManager.loadDefaultConfig();
		config.putAll(CrawlConfigManager.loadFromPath(imageConfXml));
		Integer threads = config.getInt("imageThreads",5);
		for(int i=0;i<threads;i++){
			Thread t = new Thread(new ImageConsumer(config));
			t.start();
		}
	}
}
