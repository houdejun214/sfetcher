package com.sdata.core;

import com.lakeside.core.utils.StringUtils;
import com.sdata.core.fetcher.SdataFetcher;
import org.hsqldb.lib.StringUtil;

import java.util.HashMap;
import java.util.Map;

public class RawContent {
	
	public RawContent(String content){
		this.content = content;
	}

    public RawContent(String url,String content){
		this.url = url;
		this.content = content;
	}

    public RawContent(String url, boolean lazyDownload, SdataFetcher fetcher) {
        this.url = url;
        this.lazyDownload = lazyDownload;
        this.fetcher = fetcher;
    }

    private boolean lazyDownload=false;

    private SdataFetcher fetcher;

    private String url;
	
	private String content;
	
	private Map<String,Object> metadata = null;

    public boolean isLazyDownload() {
        return lazyDownload;
    }

    public void setLazyDownload(boolean lazyDownload) {
        this.lazyDownload = lazyDownload;
    }

    public String getContent() {
		return content;
	}

	public void setContent(String content) {
		this.content = content;
	}

	public String getUrl() {
		return url;
	}

	public void setUrl(String url) {
		this.url = url;
	}
	
	public void setMetadata(String key,Object value){
		if(this.metadata==null){
			this.metadata = new HashMap();
		}
		this.metadata.put(key, value);
	}
	
	public void addAllMeata(Map<String,Object> map){
		if(this.metadata==null){
			this.metadata = new HashMap();
		}
		this.metadata.putAll(map);
	}
	
	public Object getMetadata(String key) {
		if(this.metadata!=null){
			return this.metadata.get(key);
		}
		return null;
	}

    public Map<String, Object> getMetadata() {
        return metadata;
    }

    public boolean isEmpty(){
		if(this.content==null || "".equals(content)){
			return true;
		}
		return false;
	}

    public void fetchContent() {
        if (StringUtils.isEmpty(this.content) && StringUtils.isNotEmpty(this.url)) {
            this.content = this.fetcher.fetchContent(this.url, this.metadata);
        }
    }
}
