package com.sdata.core.data.statistics;

import org.apache.commons.lang.WordUtils;

import com.sdata.core.Configuration;



/**
 * @author zhufb
 *
 */
public class StatisticsStore {
	
	private StatisticsDB sdb ;
	public StatisticsStore(Configuration config){
		sdb = new StatisticsDB(config);
	}
	
	public void increase(String souce,String collection){
		sdb.increase(WordUtils.capitalize(souce),collection);
	}
	
	public void increase(String collection){
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
