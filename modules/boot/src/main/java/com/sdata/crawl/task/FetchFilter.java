package com.sdata.crawl.task;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.sdata.core.FetchDatum;
import com.sdata.core.filter.SdataFilter;


public class FetchFilter {
	
	protected Logger log = LoggerFactory.getLogger(this.getClass());
	
	private List<SdataFilter> beforeFilters = null;
	private List<SdataFilter> afterFilters = null;
	
	public FetchFilter(List<SdataFilter> beforeFilters,List<SdataFilter> afterFilters){
		this.beforeFilters = beforeFilters;
		this.afterFilters = afterFilters;
	}
	
	/**
	 * filter the crawl datum whether it does meet the requirements
	 * 
	 * true represent that the datum is meet the requirements, otherwise it is not 
	 * @param image
	 * @return
	 */
	public boolean filterBeforeFetch(FetchDatum data){
		if(beforeFilters!=null && beforeFilters.size()>0){
			for(SdataFilter filter:beforeFilters){
				boolean flag = filter.filter(data);
				if(!flag){
					return false;
				}
			}
		}
		return true;
	}
	
	
	public boolean filterAfterFetch(FetchDatum data){
		if(afterFilters!=null && afterFilters.size()>0){
			for(SdataFilter filter:afterFilters){
				boolean flag = filter.filter(data);
				if(!flag){
					return false;
				}
			}
		}
		return true;
	}
}