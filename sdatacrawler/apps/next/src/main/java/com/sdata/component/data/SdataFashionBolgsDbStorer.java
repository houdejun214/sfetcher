package com.sdata.component.data;

import java.util.Date;
import java.util.Map;
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.lakeside.core.utils.UUIDUtils;
import com.sdata.component.data.dao.FashionBlogsMgDao;
import com.sdata.component.util.YouTubeVideoDownloadUtils;
import com.sdata.core.Configuration;
import com.sdata.core.Constants;
import com.sdata.core.FetchDatum;
import com.sdata.core.RunState;
import com.sdata.core.data.SdataStorer;

public class SdataFashionBolgsDbStorer extends SdataStorer {

	private static final Logger log = LoggerFactory.getLogger("SdataCrawler.SdataFashionBolgsDbStorer");
	
	private FashionBlogsMgDao dao = new FashionBlogsMgDao();
	
	public SdataFashionBolgsDbStorer(Configuration conf,RunState state){
		this.setConf(conf);
		this.state = state;
		String host = this.getConf("mongoHost");
		int port = this.getConfInt("mongoPort",27017);
		String dbName = this.getConf("mongoDbName");
		this.dao.initilize(host, port, dbName);
		this.state = state;
	}
	
	@Override
	public void save(FetchDatum datum) throws Exception {
		try {
			Map<String, Object> metadata = datum.getMetadata();
			metadata.put(Constants.FETCH_TIME, new Date());
			dao.saveBlogs(metadata);
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
