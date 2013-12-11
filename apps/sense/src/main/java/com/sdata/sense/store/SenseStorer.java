package com.sdata.sense.store;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

import com.framework.db.hbase.thrift.HBaseClient;
import com.framework.db.hbase.thrift.HBaseClientFactory;
import com.lakeside.core.utils.StringUtils;
import com.sdata.context.config.Configuration;
import com.sdata.context.state.RunState;
import com.sdata.core.FetchDatum;
import com.sdata.core.data.store.SdataStandardStorer;
import com.sdata.core.parser.config.StoreConfig;
import com.sdata.db.BaseDao;
import com.sdata.db.DaoCollection;
import com.sdata.db.hbase.HBaseDaoWithRawIndex;
import com.sdata.sense.SenseConfig;
import com.sdata.sense.SenseFetchDatum;
import com.sdata.sense.item.SenseCrawlItem;

/**
 * @author zhufb
 *
 */
public class SenseStorer extends SdataStandardStorer {

	public static String DEFAULT_SENSE_STORER = "sense" ;
	
	public final static String SID = null;
	
	private HBaseClient client;
	public SenseStorer(Configuration conf,RunState state) {
		super(conf,state);
	}

	//sense dont use stata standard init
	@Override
	protected void init() {
		String clusterName = super.getConf("hbase.cluster.name");
		String namespace = super.getConf("hbase.namespace");
		client = HBaseClientFactory.getClientWithCustomSeri(clusterName, namespace);
	}
	
	@Override
	public void save(FetchDatum datum) throws Exception {
		Configuration config = getConf(datum);
		Map<String, Object> prepareData = ((SenseFetchDatum)datum).defaultData();
		Iterator<DaoCollection> collections = StoreConfig.getInstance(config).getCollections();
		while(collections.hasNext()){
			DaoCollection collection = collections.next();
			BaseDao dao = getDBStore(config,collection.getName());
			this.save(collection,dao,datum.getMetadata(),prepareData);
		}
	}
	
	protected void save(DaoCollection collection,BaseDao dao,Map<String, Object> data,Map<String,Object> prepare){
		String field = collection.getField();
		// default all datum
		if(StringUtils.isEmpty(field)){
			this.save(dao,data,prepare);
			return;
		}
		Object object = data.get(field);
		if(object instanceof List){
			Iterator iterator = ((List) object).iterator();
			while(iterator.hasNext()){
				Map<String,Object> inner = (Map<String,Object>)iterator.next();
				this.save(dao,inner,prepare);
			}
		}else if(object instanceof Map){
			Map<String,Object> inner = (Map<String,Object>)object;
			this.save(dao,inner,prepare);
		}
	}
	
	protected void save(BaseDao dao,Map<String, Object> data,Map<String,Object> prepare){
		data.putAll(prepare);
		dao.save(data);
	}
	
	public boolean isExists(SenseFetchDatum datum) {
		Configuration conf = getConf(datum);
		DaoCollection sc = getMainCollection(conf);
		if(sc == null){
			return false;
		}
		HBaseDaoWithRawIndex dbStore = (HBaseDaoWithRawIndex)super.getDBStore(conf,sc.getName());
		return	dbStore.getHbaseGenID().isExists(datum.getId(),datum.getIndexColumn());
	}
	
	public DaoCollection getMainCollection(Configuration config) {
		StoreConfig instance = StoreConfig.getInstance(config);
		return instance.getMainCollection();
	}
	
	protected SenseCrawlItem getCrawlItem(FetchDatum datum){
		return ((SenseFetchDatum)datum).getCrawlItem();
	}
	
	protected Configuration getConf(FetchDatum datum){
		SenseCrawlItem item = getCrawlItem(datum);
		return SenseConfig.getConfig(item);
	}
	
}
