package com.sdata.core.data;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.sdata.core.Configuration;
import com.sdata.core.FetchDatum;
import com.sdata.core.RunState;
import com.sdata.core.data.dao.DBStore;
import com.sdata.core.data.dao.StoreCollection;
import com.sdata.core.index.es.ElasticServer;
import com.sdata.core.parser.html.config.StoreConfig;
import com.sdata.elastic.Elastic;

/**
 * @author zhufb
 *
 */
public class SdataStandardStorer extends SdataStorer {

	private static final Logger log = LoggerFactory.getLogger("SdataStandardStorer");
	protected Map<String,DBStore> storeMap = new HashMap<String,DBStore>();
	protected Elastic es;
	protected FieldProcess fieldProcess;
	public SdataStandardStorer(Configuration conf,RunState state){
		this.setConf(conf);
		this.state = state;
		this.init();
	}
	
	protected void init() {
		this.fieldProcess = initFieldProcess(getConf());
		this.storeMap = initStoreMap(getConf());
		if(isIndex(getConf())){
			this.es = initElastic(getConf());
		}
	}
	
	protected FieldProcess initFieldProcess(Configuration conf) {
		return new FieldProcess(conf);
	}

	protected Map<String,DBStore> initStoreMap(Configuration conf) {
		return DBStore.getStoreMap(conf);
	}
	
	protected boolean isIndex(Configuration conf) {
		return conf.getBoolean("crawler.index", false);
	}
	
	protected Elastic initElastic(Configuration conf) {
		String name = conf.get("elastic.cluster.name");
		if(StringUtils.isEmpty(name)){
			throw new RuntimeException("elastic.cluster.name is null!");
		}
		return ElasticServer.getElastic(name);
	}
	
	protected Map<String, Object> process(FieldProcess fieldProcess,Map<String, Object> data) {
		return fieldProcess.fieldReduce(data);
	}
	
	protected void saveOneCollection(StoreCollection collection,FieldProcess fieldProcess,DBStore dao,Map<String, Object> data){
		String field = collection.getField();
		String index = collection.getIndex();
		boolean needIndex = false;
		if(!StringUtils.isEmpty(index)){
			needIndex = Boolean.valueOf(index);
		}
		// default all datum
		if(StringUtils.isEmpty(field)){
			this.save(dao,needIndex,fieldProcess,data);
			return;
		}
		Object object = data.get(field);
		if(object instanceof List){
			Iterator iterator = ((List) object).iterator();
			while(iterator.hasNext()){
				this.save(dao,needIndex,fieldProcess,(Map<String,Object>)iterator.next());
			}
		}else if(object instanceof Map){
			this.save(dao,needIndex,fieldProcess,(Map<String, Object>) object);
		}
	}
	
	protected Map<String,Object> elasticProcess(FieldProcess fieldProcess,Map<String,Object> data) {
		return fieldProcess.elasticIndex(data);
	}
	
	protected void index(FieldProcess fieldProcess,Map<String,Object> data) {
		es.save(this.elasticProcess(fieldProcess,data));
	}
	
	protected void save(DBStore dao,boolean needIndex,FieldProcess fieldProcess,Map<String,Object> data){
		if(needIndex){
			this.index(fieldProcess,data);
		}
		dao.save(data);
	}
	
	protected void save(Configuration conf,FieldProcess fieldProcess,Map<String, Object> metadata){
		metadata = this.process(fieldProcess,metadata);
		if(metadata == null){
			return;
		}

		Iterator<StoreCollection> collections = StoreConfig.getInstance(conf).getCollections();
		while(collections.hasNext()){
			StoreCollection collection = collections.next();
			DBStore dao = getDBStore(conf,collection.getName());
			saveOneCollection(collection,fieldProcess,dao,metadata);
		}
	}
	
	
	protected void save(Map<String, Object> metadata){
		this.save(this.getConf(),this.getFieldProcess(), metadata);
	}
	
	public DBStore getDBStore(Configuration conf,String name){
		return this.initStoreMap(conf).get(name);
	}

	@Override
	public void save(FetchDatum datum) throws Exception {
		if(datum!=null && datum.getMetadata()!=null){
			this.save(this.getConf(),this.getFieldProcess(),datum.getMetadata());
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

	public FieldProcess getFieldProcess() {
		return fieldProcess;
	}

	public Elastic getEs() {
		return es;
	}
}
