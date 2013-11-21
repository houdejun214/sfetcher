package com.sdata.component.fetcher;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.lakeside.core.utils.JSONUtils;
import com.lakeside.core.utils.StringUtils;
import com.lakeside.core.utils.time.DateTimeUtils;
import com.sdata.component.site.TwitterApi;
import com.sdata.core.Configuration;
import com.sdata.core.Constants;
import com.sdata.core.FetchDatum;
import com.sdata.core.RunState;
import com.sdata.core.fetcher.SdataFetcher;
import com.sdata.core.util.EmailUtil;

public class TwitterFetcher extends SdataFetcher{
	
	private static final Logger log = LoggerFactory.getLogger("SdataCrawler.TwitterFetcher");
	
	private final TwitterApi api = new TwitterApi();
	
	private boolean startflag = false;

	
	public TwitterFetcher(Configuration conf,RunState state) {
		this.setConf(conf);
		this.setRunState(state);
		String consumerKey = conf.get("ConsumerKey");
		String consumerSecret = conf.get("ConsumerSecret");
		String accessTokenName = conf.get("AccessToken");
		String accessTokenSecret = conf.get("AccessTokenSecret");
		api.setOAuth(consumerKey, consumerSecret, accessTokenName, accessTokenSecret);
	}

	@Override
	public List<FetchDatum> fetchDatumList() {
		if(!startflag){//first time ,start twitterStream
			api.startSample();
			startflag=true;
		}
		
		List<Map> resultTweetList = api.getTweets();
		List<FetchDatum> resultList = new ArrayList<FetchDatum>();
		if(resultTweetList!=null && resultTweetList.size()>0){
			for(int i=0;i<resultTweetList.size();i++){
				try {
					Map json = resultTweetList.get(i);
					FetchDatum fetchDatum = this.transformJSONtoFetchDatum(json);
					resultList.add(fetchDatum);
				} catch (Exception e) {
					String[] emailAddress = new String[1];
					emailAddress[0] = "qiumm820@gmail.com";
					EmailUtil.send(emailAddress, "twitter fetchlist[64]: transformJSONtoFetchDatum Exception", resultTweetList.get(i).toString());
				}
			}
		}
		log.info("fetched items"+resultTweetList.size());
		return resultList;
	}
	
	/**
	 * transform  JSONObject to FetchDatum
	 * @param json
	 * @return
	 * @author qiumm
	 */
	private FetchDatum transformJSONtoFetchDatum(Map json) {
		FetchDatum datum = new FetchDatum();
		String id = StringUtils.valueOf(json.get("id"));
		datum.setId(id);
		json.put(Constants.OBJECT_ID, Long.parseLong(id));
		datum.setMetadata(json);
		return datum;
	}
	
	@Override
	public FetchDatum fetchDatum(FetchDatum datum) {
		return datum;
	}
	
	/**
	 * move to next crawl instance
	 */
	@Override
	protected void moveNext() {
	}
	
	@Override
	public boolean isComplete(){
		return false;
		
	}
	

	private String dateFormat(Date time){
		return DateTimeUtils.format(time, "yyyy-MM-dd HH:mm:ss");
	}
	
	private String getTimeSpan(Date time1,Date time2){
		return "["+DateTimeUtils.format(time1, "yyyy-MM-dd HH:mm:ss")+" - " + DateTimeUtils.format(time2, "yyyy-MM-dd HH:mm:ss")+ "]";
	}
	
	private Date parseToDate(String str){
		return DateTimeUtils.parse(str, "yyyy-MM-dd HH:mm:ss");
	}
}
