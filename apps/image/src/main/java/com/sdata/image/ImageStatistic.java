package com.sdata.image;

import com.sdata.core.Configuration;
import com.sdata.core.data.statistics.StatisticsStore;

/**
 * @author zhufb
 *
 */
public class ImageStatistic {

	private static String CollectionName = "images"; 
	private static ImageStatistic is;
	private StatisticsStore ss;
	private  ImageStatistic(Configuration conf){
		ss = new StatisticsStore(conf);
	}
	
	public void save(String source,String collection){
		ss.increase(source, collection);
	}

	public  void save(String source){
		save(source, CollectionName);
	}
	
	public static ImageStatistic getInstance(Configuration conf){
		if(is == null){
			synchronized (CollectionName) {
				if(is == null){
					is = new ImageStatistic(conf);
				}
			}
		}
		return is;
	}
}
