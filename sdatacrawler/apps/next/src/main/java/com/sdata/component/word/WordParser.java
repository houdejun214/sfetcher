package com.sdata.component.word;

import java.util.Date;
import java.util.List;

import com.sdata.component.word.tencent.TencentWordParser;
import com.sdata.component.word.twitter.TwitterWordParser;
import com.sdata.component.word.weibo.WeiboWordParser;
import com.sdata.core.Configuration;
import com.sdata.core.Constants;
import com.sdata.core.FetchDatum;
import com.sdata.core.item.CrawlItem;
import com.sdata.core.parser.SdataParser;

public abstract class WordParser extends SdataParser {

	public abstract List<FetchDatum> getFetchList(CrawlItem item,Date startTime,Date endTime,String currentState); 
	
	public abstract FetchDatum getFetchDatum(FetchDatum datum); 

	public abstract String getCurrentState();
	
	public  boolean isComplete(){
		return false;
	}

	protected void await(long millis) {
		try {
			Thread.sleep(millis);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
	
	public static WordParser getWordParser(Configuration conf){
		String source = conf.get(Constants.SOURCE);
		if("tencent".equals(source)){
			return new TencentWordParser(conf);
		}else if("weibo".equals(source)){
			return new WeiboWordParser(conf);
		}else if("twitter".equals(source)){
			return new TwitterWordParser(conf);
		}
		return null;
	}
		
}
