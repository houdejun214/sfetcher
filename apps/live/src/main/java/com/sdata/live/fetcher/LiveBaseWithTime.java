package com.sdata.live.fetcher;

import java.util.List;

import com.sdata.core.FetchDatum;
import com.sdata.live.state.LiveState;
import com.sdata.proxy.SenseFetchDatum;
import com.sdata.proxy.item.SenseCrawlItem;

/**
 * @author zhufb
 *
 */
public interface LiveBaseWithTime{
	
	public List<FetchDatum> getList(SenseCrawlItem item,LiveState state);
	
	public SenseFetchDatum getDatum(SenseFetchDatum datum);
	
	public void next(LiveState state);
	
	public boolean isComplete();
	
	public boolean isValid(String html);
	
	public void refreshResource();
}
