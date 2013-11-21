package com.sdata.index;

import java.util.Map;
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.lakeside.core.utils.UUIDUtils;
import com.mongodb.DBObject;
import com.sdata.core.Constants;
import com.sdata.core.CrawlAppContext;
import com.sdata.core.data.FieldProcess;

public class DBIndexProcess implements Runnable {
	private static final Logger log = LoggerFactory.getLogger("DBIndexProcess");
	private DBDataQueue dbQueue;
	private DBDataDao dbDao;
	private FieldProcess process;
	private FieldProcess processOther = null;
	public DBIndexProcess(DBDataQueue queue,FieldProcess process,FieldProcess processOthers,DBDataDao dbDao){
		this.dbQueue = queue;
		this.process = process;
		this.dbDao = dbDao;
		this.processOther = processOthers;
	}
	
	public void run() {
		while(true){
			DBObject dbObject = dbQueue.get();
			if(dbObject == null){
				if(DBIndexTask.isComplete()){
					this.complete();
					return;
				}else{
					continue;
				}
			}
			try{
				// get full info
				Object id = dbObject.get(Constants.OBJECT_ID);
				dbObject = dbDao.query(id);
				// image process
				Map<String, Object> map = this.processImage(dbObject);
				//db update data
				//dbDao.update(map);
				//index solr storage
				if(id instanceof UUID){
					map.put(Constants.OBJECT_ID, UUIDUtils.encode((UUID)id));
				}
				this.processIndex(map);
			}catch(Exception e){
				log.warn(" process one error:"+e.getMessage());
				e.printStackTrace();
				continue;
			}finally{
				//execute complete and add one 
				this.add();
			}
		}
	}
	
	public Map<String,Object> processImage(DBObject dbObject) {
		return  process.fieldReduce(dbObject.toMap());
	}

	public void processIndex(Map<String,Object> map) {
//		Map<String, Object> result = new HashMap<String,Object>();
		if(processOther!=null){
			map.put("origin", CrawlAppContext.conf.get(Constants.SOURCE));
			 processOther.solrIndex(map);	
		}else{
			process.solrIndex(map);
		}
	}

	private void add(){
		CrawlAppContext.state.addIndexCount(1);
	}
	
	private void complete(){
		DBIndexTask.countDown();
	}
}
