package com.sdata.core.data.store;

import com.sdata.context.config.Configuration;
import com.sdata.context.state.RunState;
import com.sdata.core.FetchDatum;

/**
 * 不做任何存储操作，仅作为测试使用。
 * 
 * @author houdejun
 *
 */
public class SdataEmptyStorer extends SdataStorer{
	
	public SdataEmptyStorer(Configuration conf,RunState state){
		this.setConf(conf);
	}

	@Override
	public void save(FetchDatum datum) throws Exception {
		
	}
	
}
