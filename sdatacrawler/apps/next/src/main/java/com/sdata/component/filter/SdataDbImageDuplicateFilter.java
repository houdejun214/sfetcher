package com.sdata.component.filter;

import com.sdata.component.data.dao.ImageMgDao;
import com.sdata.core.Configuration;
import com.sdata.core.Constants;
import com.sdata.core.FetchDatum;
import com.sdata.core.RunState;
import com.sdata.core.filter.SdataFilter;

public class SdataDbImageDuplicateFilter extends SdataFilter {

	private ImageMgDao dao = null;

	private RunState state;
	
	public SdataDbImageDuplicateFilter(Configuration conf,RunState state) {
		super(conf);
		this.state = state;
		this.dao = new ImageMgDao(conf.get(Constants.SOURCE));;
	}
	
	public boolean filter(FetchDatum datum) {
		String id = datum.getId().toString();
		if(dao.isImageExists(id)){
			state.addOneRepeatDiscard();
			return false;
		}else{
			return true;
		}
	}
}
