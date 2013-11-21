package com.sdata.component.data;

import java.net.UnknownHostException;
import java.util.Date;
import java.util.Map;

import com.mongodb.MongoException;
import com.sdata.component.data.dao.FoursquareCheckinDao;
import com.sdata.core.Configuration;
import com.sdata.core.Constants;
import com.sdata.core.FetchDatum;
import com.sdata.core.NegligibleException;
import com.sdata.core.RunState;
import com.sdata.core.data.FieldProcess;
import com.sdata.core.data.SdataStorer;

/**
 * store tweets data which contain twitter tweets and weibo tweets
 * 
 * @author houdj
 *
 */
public class SdataFoursquareDbStorer extends SdataStorer {

	//private static final Logger log = LoggerFactory.getLogger("SdataCrawler.FoursquareCheckinDbStorer");
	
	private FieldProcess fieldProcess ;
	
	private FoursquareCheckinDao checkinDao = new FoursquareCheckinDao();

	public SdataFoursquareDbStorer(Configuration conf,RunState state) throws UnknownHostException, MongoException{
		this.setConf(conf);
		String host = this.getConf("mongoHost");
		int port = this.getConfInt("mongoPort",27017);
		String dbName = this.getConf("mongoDbName");
		this.checkinDao.initilize(host, port, dbName);
		this.state = state;
		fieldProcess = new FieldProcess(conf);
	}
	
	@Override
	public void save(FetchDatum datum) throws Exception {
		Map<String, Object> metadata = datum.getMetadata();
		// save tweet
		metadata.put(Constants.FETCH_TIME, new Date());
		Boolean isCheckIn = (Boolean)metadata.remove("isCheckIn");
		if(!isCheckIn  || (!metadata.containsKey("checkinId") && !metadata.containsKey("id"))){
			throw new NegligibleException("["+metadata.get("twitterid")+"] is not a checkin data!");
		}
		metadata = fieldProcess.fieldReduce(metadata);
		checkinDao.saveCheckin(metadata,fieldProcess);
	}
}
