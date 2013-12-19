package com.sdata.crawl.cluster;

import java.util.concurrent.CountDownLatch;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooKeeper;

public class ZKConnectionWatcher implements Watcher {

	private static final int SESSION_TIMEOUT = 5000;
	
	protected ZooKeeper zk;
	
	private CountDownLatch connectedSingnal = new CountDownLatch(1);
	
	private boolean connected = false;
	
	protected void connect(String hosts){
		try {
			zk = new ZooKeeper(hosts,SESSION_TIMEOUT,this);
			connectedSingnal.await();
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}
	
	public void process(WatchedEvent event) {
		if(!connected && event.getState() == KeeperState.SyncConnected){
			connectedSingnal.countDown();
			connected = true;
		}
		switch(event.getType()) {
	      case NodeCreated:
	    	  processNodeCreated(event.getPath());
	    	  break;
	      case NodeDeleted:
	    	  processNodeDeleted(event.getPath());
	    	  break;
	      case NodeDataChanged:
	    	  processNodeDataChanged(event.getPath());
	    	  break;
	      case NodeChildrenChanged:
	    	  processNodeChildrenChanged();
	    	  break;
	      default:
	    	  break;
		}
	}

	protected void processNodeDataChanged(String path) {
		
	}

	protected void processNodeCreated(String string) {
		
	}

	protected void processNodeDeleted(String string) {
		
	}

	protected void processNodeChildrenChanged() {
		
	}
}
