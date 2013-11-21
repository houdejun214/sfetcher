package com.sdata.crawl.cluster;

import java.lang.management.ManagementFactory;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.List;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.lakeside.core.utils.StringUtils;
import com.sdata.core.Configuration;

public class CrawlWatcher extends ZKConnectionWatcher {
	
	private static final Logger log = LoggerFactory.getLogger("SdataCrawler.CrawlWatcher");

	private static final byte[] EMPTY_DATA = new byte[0];

	private static final String ROOT_CRAWLER = "/crawler";
	
	private String crawlerRoot = ROOT_CRAWLER;
	private String clusterPath;
	private final String clusterName;
	
	private final String crawlerName;
	
	private final String hosts;
	
	private final Manager manager;

	//当前节点的名称 "crawlName + hostName + processId"
	private String nodeName = null;
	
	//当前节点的路径,唯一标识节点信息
	private String nodePath = null;
	
	// 当前节点ID,zookeeper分配的，用来选取领导者
	private Integer nodeId = new Integer(-1);
	
	private boolean isLeaderNode = false;
	
	public String getClusterPath() {
		return clusterPath;
	}

	public void setClusterPath(String clusterPath) {
		this.clusterPath = clusterPath;
	}

	public String getNodeName() {
		return nodeName;
	}

	public void setNodeName(String nodeName) {
		this.nodeName = nodeName;
	}

	public String getClusterName() {
		return clusterName;
	}

	public String getCrawlerName() {
		return crawlerName;
	}

	public String getNodePath() {
		return nodePath;
	}

	public CrawlWatcher(Configuration conf,Manager manager) throws UnknownHostException {
		String root = conf.get("crawler.cluster.zk.root");
		this.hosts = conf.get("crawler.cluster.zk.hosts");
		this.clusterName = conf.get("crawler.cluster.name");
		this.crawlerName =  conf.get("crawlName");
		clusterPath = crawlerRoot+"/"+clusterName;
		this.manager = manager;
		if(!StringUtils.isEmpty(root)){
			this.crawlerRoot = StringUtils.chompLast(root,"/");
		}
	}

	public void start() {
		try {
			log.info("connecting to zookeeper server");
			this.connect(hosts);
			log.info("initialize zookeeper base nodes");
			this.initNodes();
			this.registerCurrentNode();
			log.info("register current znode [{}]",this.nodeName);
			this.determineElectionStatus();
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}
	
	private void initNodes() throws KeeperException, InterruptedException, UnknownHostException{
		// create root node
		 Stat s = zk.exists(crawlerRoot, false);  
	     if (s == null) {  
	         zk.create(crawlerRoot, EMPTY_DATA, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);  
	     }
	     
	     // create cluster node
	     s = zk.exists(clusterPath, true);
	     if (s == null) {  
	         zk.create(clusterPath, EMPTY_DATA, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);  
	     }
	}

	/**
	 * 注册当前crawler节点
	 * 
	 * @throws KeeperException
	 * @throws InterruptedException
	 * @throws UnknownHostException 
	 */
	private void registerCurrentNode() throws KeeperException,
			InterruptedException, UnknownHostException {
		// create self node
		String hostName = getHostName();
		String curProcessId = this.getCurProcessId();
		nodeName = crawlerName + ":" + hostName + ":" + curProcessId;
		String currentPath = clusterPath + "/" + nodeName+"_";
		nodePath = zk.create(currentPath, CrawlerState.Activate.getByte(), Ids.OPEN_ACL_UNSAFE,CreateMode.EPHEMERAL_SEQUENTIAL);
		int index = nodePath.lastIndexOf("_");
		if(index>-1){
			this.nodeId = Integer.valueOf(nodePath.substring(index+1));
		}
		// add watcher to current node;
		zk.exists(currentPath, this);
	}
	
	/**
	 * 选取领导者
	 */
	private void determineElectionStatus(){
		try {
			List<String> children = zk.getChildren(clusterPath, false);
			int leader = Integer.MAX_VALUE;
			for(String path:children){
				int index = path.lastIndexOf("_");
				int id = Integer.valueOf(path.substring(index+1));
				if(id < leader){
					leader = id;
				}
			}
			if(this.nodeId.equals(leader)){
				this.isLeaderNode = true;
				log.info("cluster election, current node elected as cluster leader");
			}else{
				this.isLeaderNode = false;
				log.info("cluster election, current node is a normal crawler node");
			}
			processNodeChildrenChanged();
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}
	
	private List<String> listAllCrawler(){
		try {
			List<String> children = zk.getChildren(clusterPath, this);
			return children;
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}
	
	@Override
	protected void processNodeDataChanged(String path) {
		try {
			byte[] data = zk.getData(path, this, null);
			CrawlerState state = CrawlerState.getState(CrawlerState.class, data);
			manager.updateCrawlerWithState(state);
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	protected void processNodeChildrenChanged() {
		if(isLeaderNode){
			List<String> crawlers = listAllCrawler();
			this.manager.updateCrawlers(crawlers);
		}
	}
	
	@Override
	protected void processNodeDeleted(String path) {
		if(isLeaderNode){
			String nodeName = getNodeNameByPath(path);
			log.info("node [{}] been deleted",nodeName);
			this.determineElectionStatus();
		}
	}
	
	private String getNodeNameByPath(String nodePath){
		String _nodeName = StringUtils.chompHeader(nodePath,clusterPath+"/");
		int index = _nodeName.lastIndexOf("_");
		return _nodeName.substring(0,index);
	}

	private String getHostName() throws UnknownHostException{
		InetAddress localHost = InetAddress.getLocalHost();
		String hostName = localHost.getHostName();
		if(StringUtils.isEmpty(hostName) || hostName.indexOf("localhost")>=0){
			hostName = localHost.getHostAddress();
		}
		return hostName;
	}
	
	private String getCurProcessId(){
		String pid = ManagementFactory.getRuntimeMXBean().getName();  
		String[]Ids = pid.split("@");  
		Long osProcessId = Long.valueOf(Ids[0]);
		return osProcessId.toString();
	}
	
}
