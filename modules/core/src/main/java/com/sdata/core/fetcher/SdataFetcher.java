package com.sdata.core.fetcher;

import java.util.List;

import com.sdata.context.config.SdataConfigurable;
import com.sdata.context.state.RunState;
import com.sdata.core.FetchDatum;
import com.sdata.core.FetchDispatch;
import com.sdata.core.data.store.SdataStorer;
import com.sdata.core.parser.SdataParser;

public abstract class SdataFetcher extends SdataConfigurable {
	
	protected RunState state;
	
	protected SdataParser parser;
	
	protected SdataStorer storer;
	
	public void setParser(SdataParser parser) {
		this.parser = parser;
	}
	
	protected void setRunState(RunState state){
		this.state = state;
	}
	
	public void setStorer(SdataStorer storer) {
		this.storer = storer;
	}
	
	protected void await(long millis){
		try {
			Thread.sleep(millis);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * @deprecated
	 * @return
	 */
	public List<FetchDatum> fetchDatumList(){ return null;};
	
	public void fetchDatumList(FetchDispatch dispatch){};

	public FetchDatum fetchDatum(FetchDatum datum){return null;};
	
	protected void moveNext(){};

	public boolean isComplete(){return false;};
	
	/**
	 * do something when a datum is fetched finish
	 * @param datum
	 */
	public void datumFinish(FetchDatum datum){
		// this method do nothing.
		// you can add your business in your fetcher object
	}
	
	/**
	 * do something when a crawl task start
	 */
	public void taskInitialize(){
		// this method do nothing.
		// you can add your business in your fetcher object
	}
	
	/**
	 * do something when a crawl task is finish
	 */
	public void taskFinish(){
		// this method do nothing.
		// you can add your business in your fetcher object
	}
}
