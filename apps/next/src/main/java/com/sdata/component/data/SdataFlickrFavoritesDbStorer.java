package com.sdata.component.data;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.sdata.component.data.dao.FavoritesMgDao;
import com.sdata.core.Configuration;
import com.sdata.core.FetchDatum;
import com.sdata.core.RunState;
import com.sdata.core.data.FieldProcess;
import com.sdata.core.data.SdataStorer;

/**
 * save flickr photo's favorites to database
 * 
 * @author qiumm
 *
 */
public class SdataFlickrFavoritesDbStorer extends SdataStorer {
	
	private static final Logger log = LoggerFactory.getLogger("SdataCrawler.SdataFlickrFavoritesDbStorer");
	
	private FavoritesMgDao favoritesdao = new FavoritesMgDao();
	
	private FieldProcess fieldProcess;
	
	public SdataFlickrFavoritesDbStorer(Configuration conf,RunState state){
		this.setConf(conf);
		this.state = state;
		String host = this.getConf("mongoHost");
		int port = this.getConfInt("mongoPort",27017);
		String dbName = this.getConf("mongoDbName");
		favoritesdao.initilize(host,port,dbName);
		fieldProcess = new FieldProcess(conf);
	}

	@Override
	public void save(FetchDatum datum) throws Exception {
		Map<String, Object> map = datum.getMetadata();
		map = fieldProcess.fieldReduce(map);
		try {
			favoritesdao.saveFavorite(map);
		} catch (Exception e) {
			log.error("got excepiton when save favorites,error:"+e.getMessage());
		}
	}

}
