package com.sdata.core.filter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.sdata.context.config.Configuration;
import com.sdata.context.config.SdataConfigurable;
import com.sdata.core.FetchDatum;


public class SdataFilter extends SdataConfigurable  {

    public static final Boolean CONTINUE = Boolean.TRUE;

    public static final Boolean DISCARD = Boolean.FALSE;
	
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