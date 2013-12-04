package com.sdata.core.data.store;

import com.sdata.context.config.SdataConfigurable;
import com.sdata.context.state.RunState;
import com.sdata.core.FetchDatum;

public abstract class SdataStorer extends SdataConfigurable {
	
	protected RunState state;

	public abstract void save(FetchDatum datum) throws Exception;
	
	protected void await(long millis){
		try {
			Thread.sleep(millis);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
}



