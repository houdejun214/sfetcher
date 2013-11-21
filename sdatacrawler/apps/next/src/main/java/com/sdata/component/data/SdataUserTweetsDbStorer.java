package com.sdata.component.data;

import java.net.UnknownHostException;
import java.util.Date;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.lakeside.core.utils.StringUtils;
import com.lakeside.core.utils.UUIDUtils;
import com.lakeside.core.utils.time.StopWatch;
import com.mongodb.MongoException;
import com.sdata.component.data.dao.TweetsMgDao;
import com.sdata.component.data.dao.UserMgDao;
import com.sdata.core.Configuration;
import com.sdata.core.Constants;
import com.sdata.core.FetchDatum;
import com.sdata.core.RunState;
import com.sdata.core.data.FieldProcess;
import com.sdata.core.data.SdataStorer;
import com.sdata.core.util.ApplicationContextHolder;

/**
 * store tweets data which contain twitter tweets and weibo tweets
 * 
 * @author houdj
 *
 */
public class SdataUserTweetsDbStorer extends SdataStorer {

	private static final Logger log = LoggerFactory.getLogger("SdataCrawler.SdataUserTweetsDbStorer");
	
	private TweetsMgDao tweetdao;
	
	private UserMgDao userdao;
	
	private FieldProcess fieldProcess;

	public SdataUserTweetsDbStorer(Configuration conf,RunState state) throws UnknownHostException, MongoException{
		this.setConf(conf);
		this.tweetdao = new TweetsMgDao();
		this.userdao = new UserMgDao();
		String host = this.getConf("mongoHost");
		int port = this.getConfInt("mongoPort",27017);
		String dbName = this.getConf("mongoDbName");
		this.tweetdao.initilize(host, port, dbName);
		this.userdao.initilize(host, port, dbName);
		this.state = state;
		fieldProcess = new FieldProcess(conf);
	}
	
	@Override
	public void save(FetchDatum datum) throws Exception {
		try {
			if(datum!=null){
				Map<String, Object> tweet = datum.getMetadata();
				//update user newtd
				Date newtdata = (Date)tweet.get(Constants.FAMOUS_NEWEST_TWEET_DATE);
				if(null != newtdata){
					userdao.updateNewTweetsDate(UUIDUtils.getMd5UUID((String)tweet.get("name")), newtdata);
					tweet.remove(Constants.FAMOUS_NEWEST_TWEET_DATE);
				}
				// save tweet
				tweet = fieldProcess.fieldReduce(tweet);
				tweet.put(Constants.FETCH_TIME, new Date());
				tweetdao.setSaveUser(false);
				tweetdao.saveTweet(tweet,fieldProcess,false);
			}
		} catch (Exception e) {
			logFaileMessage(e);
			throw e;
		}
	}

	protected void logFaileMessage(Exception e){
		String msg = "save failed : "+e.getMessage();
		Throwable cause = e.getCause();
		if(cause!=null){
			msg+="\r\n cause by :"+cause.getClass()+"\n"+cause.getMessage();
		}
		log.info(msg);
	}
}
