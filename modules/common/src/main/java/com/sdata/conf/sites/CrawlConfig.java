package com.sdata.conf.sites;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.sdata.context.config.Configuration;


/**
 * the site information which represent a crawl web site.
 *  
 * @author houdejun
 *
 */
public class CrawlConfig {
	private String name;
	private String hostUrl;
	private String fetcherType;
	private String parserType;
	private String storer;
	private Configuration conf;
	private Map<String,List<String>> filters = new HashMap<String,List<String>>();
	//private List<String> filters = new ArrayList<String>();
	
	public CrawlConfig(){
	}
	
	/**
	 * constructor for building a new instance.
	 * @param siteName
	 */
	public CrawlConfig(String siteName){
		this.setName(siteName);
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getHostUrl() {
		return hostUrl;
	}

	public void setHostUrl(String hostUrl) {
		this.hostUrl = hostUrl;
	}

	public String getFetcherType() {
		return fetcherType;
	}

	public void setFetcherType(String fetcherType) {
		this.fetcherType = fetcherType;
	}

	public String getParserType() {
		return parserType;
	}

	public void setParserType(String parserType) {
		this.parserType = parserType;
	}
	
	public String getStorer() {
		return storer;
	}

	public void setStorer(String storer) {
		this.storer = storer;
	}

	public Configuration getConf() {
		return conf;
	}

	public void setDefaultConf(Configuration conf) {
		this.conf = new Configuration(conf);
	}
	
	public void putConf(String key,String val){
		this.conf.put(key, val);
	}
	
	public void putAllConf(Map<String,String> map){
		this.conf.putAll(map);
	}
	
	public void addFilter(String pos,String filter){
		List<String> list = this.filters.get(pos);
		if(list == null){
			list = new ArrayList<String>();
			this.filters.put(pos, list);
		}
		list.add(filter);
	}
	
	public List<String> getFilters(String pos){
		return this.filters.get(pos);
	}
}
