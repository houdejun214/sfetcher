package com.sdata.hot.fetcher;

import com.sdata.hot.Hot;
import com.sdata.hot.Source;

/**
 * @author zhufb
 *
 */
public interface IHotFetcher {
	
	/**
	 * 标识属于某一Hot类型 
	 * 
	 * @return
	 */
	public Hot type();
	
	/**
	 * 标识属于某一数据源网站 
	 * 
	 * @return
	 */
	public Source source();
	
}
