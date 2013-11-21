package com.sdata.component.data.storer;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.sdata.core.Configuration;
import com.sdata.core.Constants;
import com.sdata.core.FetchDatum;
import com.sdata.core.RunState;
import com.sdata.core.data.SdataStandardStorer;

/**
 * 
 * tweets save
 * @author zhufb
 *
 */
public class SdataTweetsStorer extends SdataStandardStorer {

	private static final Logger log = LoggerFactory.getLogger("SdataTweetsStorer");
	public SdataTweetsStorer(Configuration conf, RunState state) {
		super(conf, state);
	}
	
	@Override
	public void save(FetchDatum datum) {
		Map<String, Object> metadata = datum.getMetadata();
		Object retweeted = metadata.remove(Constants.TWEET_RETWEETED);
		if(retweeted != null&&retweeted instanceof Map){
			super.save((Map)retweeted);
			metadata.put(Constants.TWEET_RETWEETED_ID, ((Map)retweeted).get(Constants.TWEET_ID));
		}
		super.save(metadata);
	}

}
