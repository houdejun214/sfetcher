package com.sdata.crawl.cluster;

import java.net.UnknownHostException;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.transaction.TransactionStatus;
import org.springframework.transaction.support.DefaultTransactionDefinition;

import com.lakeside.core.utils.StringUtils;
import com.lakeside.core.utils.UUIDUtils;
import com.sdata.context.config.Configuration;
import com.sdata.context.state.crawldb.CrawlDBSqlSupport;
import com.sdata.crawl.task.AbstractTask;

/**
 * 
 * crawler 集群管理类。
 * 负责实施监控集群的状态
 * 
 * @author houdejun
 *
 */
public class CrawlClusterManager extends CrawlDBSqlSupport implements Manager {
	
	private static final Logger log = LoggerFactory.getLogger("SdataCrawler.CrawlClusterManager");

	private AbstractTask task;
	
	private CrawlWatcher watcher;

	private String clusterPath;

	private String clusterName;

	public CrawlClusterManager(AbstractTask task,Configuration conf) {
		super(conf);
		this.task = task;
	}

	/**
	 * 通知Crawler集群，当前crawler进程启动。
	 * @throws UnknownHostException 
	 */
	public void startCrawler() throws UnknownHostException{
		log.info("Starting crawler cluster");
		watcher = new CrawlWatcher(conf,this);
		watcher.start();
	}
	
	/**
	 * 更新Crawler状态，
	 * 
	 * 一但有crawler进程加入或删除,CrawlWatcher 会自动回调此函数
	 */
	public void updateCrawlers(List<String> list){
		
		if(clusterPath==null)clusterPath = watcher.getClusterPath();
		if(clusterName==null)clusterName = watcher.getClusterName();
		DataSourceTransactionManager transactionManager = dataSource.getTransactionManager();
		DefaultTransactionDefinition def = dataSource.getTransactionDefinition();
		TransactionStatus transaction = transactionManager.getTransaction(def);
		try{
			// remove all records
			String sql="delete from CrawlerStatus ";
			Map<String,Object> parameters=null;
			jdbcTemplate.update(sql, parameters);
			Date now = new Date();
			
			for(String path:list){
				String _nodeName = StringUtils.chompHeader(path,clusterPath+"/");
				String[] splits = _nodeName.split(":|_");
				String crawlerName = splits[0];
				String hostName = splits[1];
				String processId = splits[2];
				String nodeName = crawlerName + ":" + hostName + ":" + processId;
				UUID id = UUIDUtils.getMd5UUID(nodeName);
				
				sql="insert into CrawlerStatus (`id`, `cluster_name`, `crawler_name`, `host_name`, `process_id`, `status`, `thread_status`, `runstate_name`,`last_update_time`) " +
						"VALUES (:id,:cluster_name,:crawler_name,:host_name,:process_id,:status,:thread_status,:runstate_name,:lastUpdateTime)";
				parameters = new HashMap<String,Object>();
				parameters.put("id", UUIDUtils.uuidToByte(id));
				parameters.put("cluster_name", clusterName);
				parameters.put("crawler_name", crawlerName);
				parameters.put("host_name", hostName);
				parameters.put("process_id", processId);
				parameters.put("status", CrawlerState.Activate.getValue());
				parameters.put("thread_status", "");
				parameters.put("runstate_name", "");
				parameters.put("lastUpdateTime", now);
				jdbcTemplate.update(sql, parameters);
			}
			transactionManager.commit(transaction);
			transaction = null;
		}finally{
			if(transaction!=null){
				transactionManager.rollback(transaction);
			}
		}
	}

	/**
	 * 
	 * 根据状态更新Crawler
	 * 
	 *  zookeeper上的状态发生变化时，CrawlWatcher 会自动回调此函数
	 */
	public void updateCrawlerWithState(CrawlerState state) {
		// 如果是停止状态，需要停止该Crawler程序
		if(CrawlerState.Stop == state){
			this.task.stop();
			// 更新数据库记录
			String nodeName = watcher.getNodeName();
			UUID id = UUIDUtils.getMd5UUID(nodeName);
			String sql="update CrawlerStatus set `status`=:status where `id`=:id";
			Map<String,Object> parameters=new HashMap<String,Object>();
			parameters.put("status", CrawlerState.Stop.getValue());
			parameters.put("id", id);
			jdbcTemplate.update(sql, parameters);
		}
	}
}
