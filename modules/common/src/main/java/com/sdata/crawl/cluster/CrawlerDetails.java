package com.sdata.crawl.cluster;

public class CrawlerDetails {
	
	private String name;
	
	private String host;
	
	private String processId;
	
	private String nodeName = null;

	public String getNodeName() {
		return nodeName;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getHost() {
		return host;
	}

	public void setHost(String host) {
		this.host = host;
	}

	public String getProcessId() {
		return processId;
	}

	public void setProcessId(String processId) {
		this.processId = processId;
	}

	public void setNodeName(String nodeName) {
		this.nodeName = nodeName;
	}

	public CrawlerDetails(String nodeName) {
		this.nodeName = nodeName;
	}

	public CrawlerDetails(String crawlerName, String hostName,
			String curProcessId) {
		this.name = crawlerName;
		this.host = hostName;
		this.processId = curProcessId;
		this.nodeName = crawlerName + ":" + hostName + ":" + curProcessId;
	}
}
