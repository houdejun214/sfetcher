package com.sdata.core.data.store;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;

import com.sdata.context.config.Configuration;
import com.sdata.context.state.RunState;
import com.sdata.core.FetchDatum;
import com.sdata.core.data.dao.DaoFactory;
import com.sdata.core.parser.config.StoreConfig;
import com.sdata.db.BaseDao;
import com.sdata.db.DaoCollection;

/**
 *  sdata crawler standard storer
 * 
 * @author zhufb
 *
 */
public class SdataBaseStorer extends SdataStorer {
	
	public SdataBaseStorer(Configuration conf,RunState state){
		this.setConf(conf);
		this.state = state;
	}

	
	/* 
	 * main method this class for save fetch datum
	 * 
	 * (non-Javadoc)
	 * @see com.sdata.core.data.store.SdataStorer#save(com.sdata.core.FetchDatum)
	 */
	@Override
	public void save(FetchDatum datum)  {
		if(datum!=null && datum.getMetadata()!=null){
			Configuration conf = this.getConf();
			Iterator<DaoCollection> collections = StoreConfig.getInstance(conf).getCollections();
			this.saveMultiCollection(conf,collections,datum.getMetadata());
		}
	}

	/**
	 * get Collection Dao with collection name 
	 * 
	 * @param conf
	 * @param name
	 * @return
	 */
	public BaseDao getDao(Configuration conf,String name){
		return DaoFactory.getDaos(conf).get(name);
	}
	
	/**
	 * 
	 * with conf meta data
	 * 
	 * @param conf
	 * @param metadatal
	 */
	protected void saveMultiCollection(Configuration conf,Iterator<DaoCollection> collections,Map<String, Object> metadata){
		while(collections.hasNext()){
			DaoCollection collection = collections.next();
			BaseDao dao = getDao(conf,collection.getName());
			this.saveOneCollection(collection,dao,metadata);
		}
	}

	/**
	 * save one collection 
	 * 
	 * @param collection
	 * @param dao
	 * @param data
	 */
	protected void saveOneCollection(DaoCollection collection,BaseDao dao,Map<String, Object> data){
		String field = collection.getField();
		// default all datum
		if(StringUtils.isEmpty(field)){
			this.save(dao,data);
			return;
		}
		Object object = data.get(field);
		if(object instanceof List){
			Iterator iterator = ((List) object).iterator();
			while(iterator.hasNext()){
				this.save(dao,(Map<String,Object>)iterator.next());
			}
		}else if(object instanceof Map){
			this.save(dao,(Map<String, Object>) object);
		}
	}
	
	/**
	 * save map data to the collectin with dao
	 * 
	 * @param dao
	 * @param data
	 */
	protected void save(BaseDao dao,Map<String,Object> data){
		dao.save(data);
	}
	
}
