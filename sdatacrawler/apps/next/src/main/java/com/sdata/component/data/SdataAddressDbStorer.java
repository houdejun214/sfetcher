package com.sdata.component.data;

import java.net.UnknownHostException;
import java.util.Date;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.MongoException;
import com.sdata.component.data.dao.AddressDao;
import com.sdata.core.Configuration;
import com.sdata.core.Constants;
import com.sdata.core.FetchDatum;
import com.sdata.core.RunState;
import com.sdata.core.data.FieldProcess;
import com.sdata.core.data.SdataStorer;
import com.sdata.core.util.ApplicationContextHolder;

public class SdataAddressDbStorer extends SdataStorer {
	private static final Logger log = LoggerFactory
			.getLogger("SdataCrawler.SdaaSdataAddressDbStorer");
	private AddressDao dao;
	private FieldProcess fieldProcess;
	public SdataAddressDbStorer(Configuration conf, RunState state)
			throws UnknownHostException, MongoException {
		this.setConf(conf);
		this.dao = new AddressDao();
		String host = this.getConf("mongoHost");
		int port = this.getConfInt("mongoPort",27017);
		String dbName = this.getConf("mongoDbName");
		this.dao.initilize(host, port, dbName);
		this.state = state;
		this.state.getCycle();
		fieldProcess = new FieldProcess(conf);
	}

	@Override
	public void save(FetchDatum datum) throws Exception {
		log.info("store datum: "+datum.getName());
		try {
			Map metadata = datum.getMetadata();
			metadata.put(Constants.FETCH_TIME, new Date());
			dao.insert(fieldProcess.fieldReduce(metadata), fieldProcess);
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
