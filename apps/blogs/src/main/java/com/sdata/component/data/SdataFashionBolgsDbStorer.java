package com.sdata.component.data;

import com.lakeside.data.sqldb.MysqlDataSource;
import com.sdata.component.data.dao.FashionBlogImageDao;
import com.sdata.core.Configuration;
import com.sdata.core.Constants;
import com.sdata.core.FetchDatum;
import com.sdata.core.RunState;
import com.sdata.core.data.SdataStorer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.Map;

public class SdataFashionBolgsDbStorer extends SdataStorer {

	private static final Logger log = LoggerFactory.getLogger("SdataCrawler.SdataFashionBolgsDbStorer");
	
	private final MysqlDataSource datasource;
	
	private final FashionBlogImageDao dao;
	
	public SdataFashionBolgsDbStorer(Configuration conf,RunState state){
		this.setConf(conf);
		this.state = state;
		String jdbcUrl = conf.get("image.store.jdbc.url");
		String username = conf.get("image.store.jdbc.username");
		String password = conf.get("image.store.jdbc.password");
		datasource = new MysqlDataSource(jdbcUrl, username, password);
		dao = new FashionBlogImageDao(datasource);
	}
	
	@Override
	public void save(FetchDatum datum) throws Exception {
		try {
			Map<String, Object> metadata = datum.getMetadata();
			metadata.put(Constants.FETCH_TIME, new Date());
			dao.save(metadata);
			
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
