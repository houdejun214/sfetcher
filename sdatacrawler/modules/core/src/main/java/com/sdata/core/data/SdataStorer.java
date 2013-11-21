package com.sdata.core.data;

import com.sdata.core.FetchDatum;
import com.sdata.core.RunState;
import com.sdata.core.SdataConfigurable;

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



