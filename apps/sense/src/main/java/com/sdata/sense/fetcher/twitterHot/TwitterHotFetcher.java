package com.sdata.sense.fetcher.twitterHot;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.sdata.core.Configuration;
import com.sdata.core.FetchDispatch;
import com.sdata.core.RunState;
import com.sdata.core.parser.ParseResult;
import com.sdata.sense.SenseConfig;
import com.sdata.sense.SenseFetchDatum;
import com.sdata.sense.fetcher.SenseFetcher;
import com.sdata.sense.item.SenseCrawlItem;
import com.sdata.sense.parser.twitterHot.TwitterHotParser;

/**
 * @author zhufb
 *
 */
public class TwitterHotFetcher extends SenseFetcher {
	public final static String FID = "twitterHot";
//	private TwitterApi api;
//	private String UIDS = "uids";
	protected static final Logger log = LoggerFactory.getLogger("Sense.TwitterHotFetcher");
	public TwitterHotFetcher(Configuration conf,RunState state) {
		super(conf,state);
		super.parser = new TwitterHotParser(conf);
	}

	@Override
	public void fetchDatumList(FetchDispatch fetchDispatch,SenseCrawlItem crawlItem) {
		Configuration conf = SenseConfig.getConfig(crawlItem);
		ParseResult result = parser.parseCrawlItem(conf, null, crawlItem);
		fetchDispatch.dispatch(result.getFetchList());
		
//		Configuration conf = SenseConfig.getConfig(crawlItem);
//		String consumerKey = conf.get("ConsumerKey");
//		String consumerSecret = conf.get("ConsumerSecret");
//		String accessTokenName = conf.get("AccessToken");
//		String accessTokenSecret = conf.get("AccessTokenSecret");
//		api = new TwitterApi(consumerKey, consumerSecret, accessTokenName, accessTokenSecret);
//		
//		Object uids = crawlItem.getParams().remove(UIDS);
//		if(uids == null){
//			return;
//		}
//		String[] uidList = uids.toString().split(",");
//		
//		long[] users = new long[uidList.length];
//		for(int i=0;i<uidList.length;i++){
//			users[i] = Long.valueOf(uidList[i]);
//		}
//		// users
//		FilterQuery query = new FilterQuery();
//		query.follow(users);
//		api.startFilter(query);
//		while(true){
//			List<Map> tweets = api.getTweets();
//			for(Map t:tweets){
//				SenseFetchDatum datum = new SenseFetchDatum();
//				String oid = SenseIDBuilder.build(crawlItem, StringUtils.valueOf(t.get(Constants.TWEET_ID)));
//				t.put(Constants.OBJECT_ID, oid);
//				datum.addAllMetadata(t);
//				datum.setId(oid);
//				datum.setCrawlItem(crawlItem);
//				fetchDispatch.dispatch(datum);
//			}
//			System.out.println("twitter hot fetch tweets: "+tweets.size());
//		}
	}
	
	@Override
	public SenseFetchDatum fetchDatum(SenseFetchDatum datum){
		if(datum == null){
			return null;
		}
		return datum;
	}
}