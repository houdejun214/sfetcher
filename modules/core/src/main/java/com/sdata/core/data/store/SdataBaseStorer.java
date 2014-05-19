package com.sdata.core.data.store;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

import com.sdata.db.Collection;
import org.apache.commons.lang.StringUtils;

import com.sdata.context.config.Configuration;
import com.sdata.context.state.RunState;
import com.sdata.core.FetchDatum;
import com.sdata.db.DaoFactory;
import com.sdata.core.parser.config.StoreConfig;
import com.sdata.db.BaseDao;

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
			Iterator<Collection> collections = StoreConfig.getInstance(conf).getCollections();
			this.saveMultiCollection(conf,collections,datum.getMetadata());
		}
	}

	/**
	 * 
	 * with conf meta data
	 * 
	 * @param conf
	 */
	protected void saveMultiCollection(Configuration conf,Iterator<Collection> collections,Map<String, Object> metadata){
		while(collections.hasNext()){
			Collection collection = collections.next();
            BaseDao dao = DaoFactory.getDao(conf,collection);
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
	protected void saveOneCollection(Collection collection,BaseDao dao,Map<String, Object> data){
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
