package com.sdata.component.data;

import java.net.UnknownHostException;
import java.util.Date;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.MongoException;
import com.sdata.component.data.dao.NewsMgDao;
import com.sdata.core.Configuration;
import com.sdata.core.Constants;
import com.sdata.core.FetchDatum;
import com.sdata.core.RunState;
import com.sdata.core.data.FieldProcess;
import com.sdata.core.data.SdataStorer;
import com.sdata.core.util.ApplicationContextHolder;

/**
 * store news data
 * 
 * @author qmm
 *
 */
public class SdataNewsDbStorer extends SdataStorer {

	private static final Logger log = LoggerFactory.getLogger("SdataCrawler.SdataNewsDbStorer");
	
	private NewsMgDao newsdao;
	
	private FieldProcess fieldProcess ;

	public SdataNewsDbStorer(Configuration conf,RunState state) throws UnknownHostException, MongoException{
		this.setConf(conf);
		this.newsdao = new NewsMgDao();
		String host = this.getConf("mongoHost");
		int port = this.getConfInt("mongoPort",27017);
		String dbName = this.getConf("mongoDbName");
		this.newsdao.initilize(host, port, dbName);
		this.state = state;
		fieldProcess = new FieldProcess(conf);
	}
	
	@Override
	public void save(FetchDatum datum) throws Exception {
		try {
			Map<String, Object> news = datum.getMetadata();
			// save news
//			news = fieldProcess.fieldReduce(news);
			news.put(Constants.FETCH_TIME, new Date());
			newsdao.saveNews(news, fieldProcess);
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
