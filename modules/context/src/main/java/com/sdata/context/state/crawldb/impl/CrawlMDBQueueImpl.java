package com.sdata.context.state.crawldb.impl;

import com.google.common.collect.Lists;
import com.lakeside.core.utils.ApplicationResourceUtils;
import com.lakeside.core.utils.Assert;
import com.lakeside.core.utils.FileUtils;
import com.sdata.context.config.Configuration;
import com.sdata.context.state.crawldb.CrawlDBQueue;
import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;

/**
 * mapdb db base crawl queue,
 * @see <a href="http://www.mapdb.org/" />
 * @author dejun
 */
public class CrawlMDBQueueImpl implements CrawlDBQueue {

	private static final Logger log = LoggerFactory.getLogger("SdataCrawler.CrawlDBQueueImpl");
    private static final String FETCH_QUEUE = "crawl_queue";
    private BlockingQueue<Map<String,Object>> queue;
    private final DB db;

    public CrawlMDBQueueImpl(Configuration conf) {
        String dir = ApplicationResourceUtils.getResourceUrl(conf.get("crawl.queue.mdb.dir"));
        Assert.hasText(dir, "berkeley db data directory can not empty");
        FileUtils.insureFileDirectory(dir);
        FileUtils.mkDirectory(dir);
        // configure and open database using builder pattern.
        // all options are available with code auto-completion.
        db = DBMaker.newFileDB(new File(dir, "queue.mdb"))
                .closeOnJvmShutdown()
                .make();
        queue = db.getStack(FETCH_QUEUE);
    }

    /* (non-Javadoc)
     * @see com.sdata.core.CrawlDB#queryQueue(int)
     */
	public List<Map<String,Object>> queryQueue(int topN){
		return queryQueue(topN,false);
	}
	
	/* 
	 * @see com.sdata.core.CrawlDB#queryQueue(int, java.lang.Boolean)
	 */
	public List<Map<String,Object>> queryQueue(String tableName,int topN){
        throw new RuntimeException("doesn't support this operation in bdb crawl queue");
	}
	
	public List<Map<String,Object>> queryQueue(String tableName,int topN,int lastId){
        throw new RuntimeException("doesn't support this operation in bdb crawl queue");
	}
	
	/* (non-Javadoc)
	 * @see com.sdata.core.CrawlDB#queryQueue(int, java.lang.String)
	 */
	public List<Map<String,Object>> queryQueue(int topN,String dispose){
		return queryQueue(topN,Boolean.valueOf(dispose));
	}
	
	/* (non-Javadoc)
	 * @see com.sdata.core.CrawlDB#queryQueueTotoalCount()
	 */
	public int queryQueueTotoalCount(){
		return this.queue.size();
	}
	
	/* 
	 * @see com.sdata.core.CrawlDB#queryQueue(int, java.lang.Boolean)
	 */
	public List<Map<String,Object>> queryQueue(int topN,Boolean dispose) {
        throw new RuntimeException("doesn't support this operation in bdb crawl queue");
    }

    public List<Map<String,Object>> queryQueue(String tableName, int topN,Boolean dispose){
        throw new RuntimeException("doesn't support this operation in bdb crawl queue");
	}
	
	public List<Map<String,Object>> queryQueueByStatus(int topN,String status){
        throw new RuntimeException("doesn't support this operation in bdb crawl queue");
	}
	
	/* (non-Javadoc)
	 * @see com.sdata.core.CrawlDB#updateQueueDispose(java.lang.String)
	 */
	public Boolean updateQueueComplete(final String key){
        throw new RuntimeException("doesn't support this operation in bdb crawl queue");
	}

    @Override
    public Map<String, Object> poll() {
        Map<String, Object> poll = this.queue.poll();
        db.commit();
        return poll;
    }

    @Override
    public Map<String, Object> peek() {
        return this.queue.peek();
    }

    /* (non-Javadoc)
         * @see com.sdata.core.CrawlDB#updateQueueDispose(java.lang.String)
         */
	public Boolean updateQueueStatus(final String key,String status){
        throw new RuntimeException("doesn't support this operation in bdb crawl queue");
	}
	
	public Boolean changeQueueStatus(String oldStatus,String newStatus){
        throw new RuntimeException("doesn't support this operation in bdb crawl queue");
	}
	
	/* (non-Javadoc)
	 * @see com.sdata.core.CrawlDB#insertQueueObjects(java.util.List)
	 */
	public Boolean insertQueueObjects(final List<Map<String,Object>> objects) {
        for (Map<String, Object> data : objects) {
            this.queue.offer(data);
        }
        db.commit();
        return true;
    }

    @Override
    public boolean insertTopQueueObjects(List<Map<String, Object>> objects) {
        for (Map<String, Object> data : objects) {
            this.queue.offer(data);
        }
        db.commit();
        return true;
    }

    /* (non-Javadoc)
         * @see com.sdata.core.CrawlDB#deleteQueueByKey(java.lang.String)
         */
	public Boolean deleteQueueByKey(String key){
        throw new RuntimeException("doesn't support this operation in bdb crawl queue");
	}

	/* (non-Javadoc)
	 * @see com.sdata.core.CrawlDB#deleteQueueByKey(java.lang.String)
	 */
	public void deleteQueue(){
		this.queue.clear();
	}
	
	/* (non-Javadoc)
	 * @see com.sdata.core.CrawlDB#isQueueDepthComplete(java.lang.String)
	 */
	public Boolean isQueueDepthComplete(final String depth){
		int result = this.queue.size();
		if(result>0){
			return false;
		}else{
			//当前depth下没有数据时，认为此depth没有完成
			return false;
		}
	}

	public void resetQueueStatus() {
        this.queue.clear();
	}
}
