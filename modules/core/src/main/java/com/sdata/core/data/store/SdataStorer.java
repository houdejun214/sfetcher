package com.sdata.core.data.store;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.sdata.context.config.SdataConfigurable;
import com.sdata.context.state.RunState;
import com.sdata.core.FetchDatum;

public abstract class SdataStorer extends SdataConfigurable {

	protected static final Logger log = LoggerFactory.getLogger("SdataStorer");
	
	protected RunState state;

	/**
	 * one datum save
	 * 
	 * @param datum
	 * @throws Exception
	 */
	public abstract void save(FetchDatum datum) throws Exception;
	
}



