package com.sdata.extension.statistic;

import org.apache.commons.lang.WordUtils;

import com.sdata.context.config.Configuration;

/**
 * @author zhufb
 *
 */
public class Statistic {

	private StatisticsDB sdb ;
	private boolean statistic;
	public Statistic(Configuration config){
		this.statistic = config.getBoolean("crawler.stastisticEnable", false);
		this.sdb = new StatisticsDB(config);
		
	}
	
	public void increase(String souce,String collection){
		if(!statistic){
			return;
		}
		sdb.increase(WordUtils.capitalize(souce),collection);
	}
	
	public void increase(String collection){
		if(!statistic){
			return;
		}
		int i = collection.indexOf("_");
		if(i < 0){
			sdb.increase(collection);
			return;
		}
		String sname = collection.substring(0,i);
		String cname = collection.substring(i+1);
		increase(sname,cname);
	}
}
