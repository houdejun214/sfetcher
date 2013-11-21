package com.sdata.core.filter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.sdata.core.Configuration;
import com.sdata.core.FetchDatum;
import com.sdata.core.SdataConfigurable;


public class SdataFilter extends SdataConfigurable  {
	
	protected Logger log = LoggerFactory.getLogger(this.getClass());
	
	public SdataFilter(Configuration conf){
		setConf(conf);
	}
	
	/**
	 * filter the crawl datum whether it does meet the requirements
	 * 
	 * true represent that the datum is meet the requirements, otherwise it is not 
	 * @param image
	 * @return
	 */
	public boolean filter(FetchDatum data){
		return true;
	}
}