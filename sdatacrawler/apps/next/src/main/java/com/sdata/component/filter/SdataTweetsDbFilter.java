package com.sdata.component.filter;


import com.lakeside.core.utils.StringUtils;
import com.sdata.core.Configuration;
import com.sdata.core.FetchDatum;
import com.sdata.core.RunState;
import com.sdata.core.filter.SdataFilter;

public class SdataTweetsDbFilter extends SdataFilter {
	private RunState state;
	private String nonEmptyField = "text";
	
	public SdataTweetsDbFilter(Configuration conf,RunState state) {
		super(conf);
		this.state = state;
	}
	
	public boolean filter(FetchDatum datum) {
		//non empty field check if null don't save
		Object object = datum.getMetadata().get(nonEmptyField);
		if(object instanceof String && StringUtils.isEmpty(String.valueOf(object))){
			state.addOneUnexpectedDiscard();
			return false;
		}
		//repeat check if repeat don't save
		//String id = datum.getId();
		//boolean tweetExists = dao.isTweetExists(id);
		//if(tweetExists){
			//state.addOneRepeatDiscard();
		//}else{
			//state.addOneSuccess();
		//}
		return true;
	}
}
