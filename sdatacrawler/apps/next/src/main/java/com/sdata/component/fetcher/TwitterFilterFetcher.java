package com.sdata.component.fetcher;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

import net.sf.json.JSONObject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import twitter4j.FilterQuery;

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

public class TwitterFilterFetcher extends SdataFetcher{
	
	private static final Logger log = LoggerFactory.getLogger("SdataCrawler.TwitterFetcher");
	
	private final TwitterApi api = new TwitterApi();
	
	private boolean startflag = false;

	private String locationStr;
	private String trackStr;
	
	public TwitterFilterFetcher(Configuration conf,RunState state) {
		this.setConf(conf);
		this.setRunState(state);
		String consumerKey = conf.get("ConsumerKey");
		String consumerSecret = conf.get("ConsumerSecret");
		String accessTokenName = conf.get("AccessToken");
		String accessTokenSecret = conf.get("AccessTokenSecret");
		locationStr = conf.get("filterLocation");
		trackStr = conf.get("filterTrack");
		api.setOAuth(consumerKey, consumerSecret, accessTokenName, accessTokenSecret);
	}

	@Override
	public List<FetchDatum> fetchDatumList() {
		if(!startflag){//first time ,start twitterStream
			if(StringUtils.isEmpty(locationStr)){
				throw new RuntimeException("location filter is empty!");
			}
			String[] locations = locationStr.split(",");
			double[][] locationFilter = new double[locations.length/2][2];
			for(int i=0;i<locations.length;i++){
				String location=locations[i];
				locationFilter[i/2][i%2] = Double.valueOf(location);
			}
			FilterQuery query = new FilterQuery();
			if(!StringUtils.isEmpty(trackStr)){
				String[] tracks = trackStr.split(",");
				query.track(tracks);
			}
			query.locations(locationFilter);
			api.startFilter(query);
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
					EmailUtil.send(emailAddress, "twitter fetchlist[78]: transformJSONtoFetchDatum Exception", resultTweetList.get(i).toString());
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
	private FetchDatum transformJSONtoFetchDatum(Map map) {
		FetchDatum datum = new FetchDatum();
		String id =StringUtils.valueOf(map.get("id"));
		datum.setId(id);
		map.put(Constants.OBJECT_ID, Long.parseLong(id));
		map.put("filt", "singapore");
		datum.setMetadata(map);
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
