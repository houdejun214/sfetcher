package com.sdata.future.weibo;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.lakeside.core.utils.MapUtils;
import com.lakeside.core.utils.StringUtils;
import com.lakeside.data.redis.RedisDB;
import com.sdata.core.Configuration;
import com.sdata.core.Constants;
import com.sdata.core.FetchDatum;
import com.sdata.core.RawContent;
import com.sdata.core.crawldb.CrawlDBRedis;
import com.sdata.core.parser.ParseResult;
import com.sdata.future.FutureIDBuilder;
import com.sdata.sense.SenseFetchDatum;
import com.sdata.sense.item.SenseCrawlItem;
import com.sdata.sense.parser.SenseParser;

/**
 * @author zhufb
 *
 */
public class WeiboFutureParser extends SenseParser {
	protected static Logger log = LoggerFactory.getLogger("Future.WeiboFutureParser");
	public WeiboFutureParser(Configuration conf) {
		super(conf);
	}

	@Override
	public ParseResult parseCrawlItem(Configuration conf, RawContent rc,
			SenseCrawlItem item) {
		ParseResult result = new ParseResult();
		RedisDB redisDB = CrawlDBRedis.getRedisDB(conf,"WeiboFuture");
		String uid = item.parse();
		String sinceId = redisDB.get(uid);
		int count = 100;
		if(StringUtils.isEmpty(sinceId)||!StringUtils.isNum(sinceId)||"0".equals(sinceId)){
			sinceId = "0";
			count = 2;
		}
		List<Map<String, Object>>  tweets = WeiboAPI.getInstance().fetchTweetsByUserId(uid, Long.valueOf(sinceId),count);
		if(tweets!=null&&tweets.size() > 0){
			Collections.sort(tweets,new TweetsComparator());
			String newid = StringUtils.valueOf(tweets.get(0).get(Constants.TWEET_ID));
			if(Long.valueOf(newid)> Long.valueOf(sinceId)){
				redisDB.set(uid, newid);
			}
		}
		result.setFetchList(parseMapToDatum(tweets,item));
		log.warn("fetch weibo uid:"+uid+",sinceId " + sinceId +",fetch tweets size:"+tweets.size());
		return result;
	}
	

	class TweetsComparator implements Comparator{
		 public int compare(Object arg0, Object arg1) {
			Long t0 = Long.valueOf(((Map)arg0).get(Constants.TWEET_ID).toString());
			Long t1 = Long.valueOf(((Map)arg1).get(Constants.TWEET_ID).toString());
			return t1.compareTo(t0);
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
				map.put(Constants.TWEET_SOURCE_RETWEETED_COUNT, map2.get("reposts_count"));
			}
			result.add(this.parseMapToDatum(map, item));
		}
		return result;
	}
	
	private SenseFetchDatum parseMapToDatum(Map<String, Object> map,SenseCrawlItem item){
		SenseFetchDatum datum = new SenseFetchDatum();
		Map cleanMap = MapUtils.cleanMap(map);
		Long id = Long.valueOf(StringUtils.valueOf( map.get(Constants.TWEET_ID)));
		byte[] oid = FutureIDBuilder.build(item,id);
		cleanMap.put(Constants.OBJECT_ID, oid);
		datum.setId(oid);
		datum.setCrawlItem(item);
		datum.addAllMetadata(cleanMap);
		return datum;
	}
}
