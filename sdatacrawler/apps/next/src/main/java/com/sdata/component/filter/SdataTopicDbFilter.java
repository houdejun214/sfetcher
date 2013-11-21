package com.sdata.component.filter;


import com.lakeside.core.utils.StringUtils;
import com.sdata.component.data.dao.TopicMgDao;
import com.sdata.component.data.dao.TweetsMgDao;
import com.sdata.core.Configuration;
import com.sdata.core.FetchDatum;
import com.sdata.core.RunState;
import com.sdata.core.filter.SdataFilter;
import com.sdata.core.util.ApplicationContextHolder;

public class SdataTopicDbFilter extends SdataFilter {
	private TweetsMgDao tweetsDao;
	private TopicMgDao topicDao;
	private RunState state;
	private String nonEmptyField = "text";
	
	public SdataTopicDbFilter(Configuration conf,RunState state) {
		super(conf);
		this.tweetsDao = new TweetsMgDao();
		this.topicDao = new TopicMgDao();
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
		String id = datum.getId().toString();
		boolean tweetExists = tweetsDao.isTweetExists(id);
		if(tweetExists){
			state.addOneRepeatDiscard();
			return false;
		}else{
			state.addOneSuccess();
			return true;
		}
	}
}
