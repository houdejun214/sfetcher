package com.sdata.sense.parser.twitterHot;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import twitter4j.TwitterException;

import com.lakeside.core.utils.StringUtils;
import com.lakeside.data.redis.RedisDB;
import com.sdata.core.Configuration;
import com.sdata.core.Constants;
import com.sdata.core.FetchDatum;
import com.sdata.core.RawContent;
import com.sdata.core.RunState;
import com.sdata.core.crawldb.CrawlDBRedis;
import com.sdata.core.parser.ParseResult;
import com.sdata.sense.SenseFetchDatum;
import com.sdata.sense.SenseIDBuilder;
import com.sdata.sense.fetcher.twitterHot.TwitterApi;
import com.sdata.sense.item.SenseCrawlItem;
import com.sdata.sense.parser.SenseParser;

/**
 * @author zhufb
 *
 */
public class TwitterHotParser extends SenseParser {

	protected static Logger log = LoggerFactory.getLogger("Sense.TwitterHotParser");
	private static TwitterApi api;
	private String UID = "dtf_uid";
	private static Object syn = new Object();
	public TwitterHotParser(Configuration conf) {
		super(conf);
	}

	@Override
	public ParseResult parseCrawlItem(Configuration conf, RawContent rc,
			SenseCrawlItem item) {
		ParseResult result = new ParseResult();
		TwitterAPI.init(conf);
		RedisDB redisDB = CrawlDBRedis.getRedisDB(conf,"TwitterHot");
		String uid = StringUtils.valueOf(item.getParam(UID));
		String lastid = redisDB.get(uid);
		List<Map<String, Object>> list = new ArrayList<Map<String,Object>>();
		try {
			if(StringUtils.isEmpty(lastid)){
				list = api.getUserTimeline(Long.valueOf(uid));
			}else{
				list = api.getUserTimeline(Long.valueOf(uid), Long.valueOf(lastid));
			}
		} catch (NumberFormatException e) {
//			e.printStackTrace();
		} catch (TwitterException e) {
//			e.printStackTrace();
		}
		//
		if(list.size() >0){
			lastid = StringUtils.valueOf(list.get(0).get(Constants.TWEET_ID));
		}
		if(StringUtils.isEmpty(lastid)){
			log.error("user:"+uid +" maybe not exists!");
		}else{
			result.setFetchList(parseMapToDatum(list,item));
			redisDB.set(uid, lastid);
			log.warn("fetch twitter uid:"+uid+",fetch tweets size:"+result.getListSize());
		}
		this.wait(5);
		return result;
	}
	
	private void wait(int s){
		try {
			Thread.sleep(s*1000);
		} catch (InterruptedException e) {
			throw new RuntimeException("twitter waiting exception",e);
		}
	}
	
	private List<FetchDatum> parseMapToDatum(List<Map<String, Object>> list,SenseCrawlItem item){
		List<FetchDatum> result = new ArrayList<FetchDatum>();
		for(Map<String,Object> map:list){
			Object retweeted = map.remove(Constants.TWEET_RETWEETED);
			if(retweeted != null&&retweeted instanceof Map){
				Map map2 = (Map)retweeted;
				result.add(this.parseMapToDatum(map2, item));
				map.put(Constants.TWEET_RETWEETED_ID, map2.get(Constants.TWEET_ID));
				map.put(Constants.TWEET_SOURCE_RETWEETED_COUNT, map2.get("retweet_count"));
			}
			result.add(this.parseMapToDatum(map, item));
		}
		return result;
	}
	
	private SenseFetchDatum parseMapToDatum(Map<String, Object> map,SenseCrawlItem item){
		SenseFetchDatum datum = new SenseFetchDatum();
		String oid = SenseIDBuilder.build(item,StringUtils.valueOf(map.get(Constants.TWEET_ID)));
		map.put(Constants.OBJECT_ID, oid);
		datum.setId(oid);
		datum.setCrawlItem(item);
		datum.addAllMetadata(map);
		return datum;
	}
	
	static class TwitterAPI{
		public static void init(Configuration conf){
			if(api == null){
				synchronized (syn) {
					if(api == null){
						String consumerKey = conf.get("ConsumerKey");
						String consumerSecret = conf.get("ConsumerSecret");
						String AccessToken = conf.get("AccessToken");
						String AccessTokenSecret = conf.get("AccessTokenSecret");
						api = new TwitterApi(consumerKey, consumerSecret, AccessToken, AccessTokenSecret);
					}
				}
			}
		}
	}

}