package com.sdata.core.fetcher;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import com.lakeside.core.utils.QueryUrl;
import com.lakeside.core.utils.StringUtils;
import com.lakeside.core.utils.URIEncoder;
import com.lakeside.download.http.HttpPage;
import com.lakeside.download.http.HttpPageLoader;
import com.sdata.context.config.Constants;
import com.sdata.context.config.SdataConfigurable;
import com.sdata.context.state.RunState;
import com.sdata.core.FetchDatum;
import com.sdata.core.FetchDispatch;
import com.sdata.core.RawContent;
import com.sdata.core.data.store.SdataStorer;
import com.sdata.core.parser.SdataParser;
import org.apache.commons.httpclient.HttpStatus;
import org.apache.http.client.utils.URIUtils;
import org.apache.http.client.utils.URLEncodedUtils;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;

public abstract class SdataFetcher extends SdataConfigurable {

    protected static HttpPageLoader advancePageLoader = HttpPageLoader.getAdvancePageLoader();
	
	protected RunState state;
	
	protected SdataParser parser;
	
	protected SdataStorer storer;
	
	public void setParser(SdataParser parser) {
		this.parser = parser;
	}
	
	protected void setRunState(RunState state){
		this.state = state;
	}
	
	public void setStorer(SdataStorer storer) {
		this.storer = storer;
	}
	
	protected void await(long millis){
		try {
			Thread.sleep(millis);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * @deprecated
	 * @return
	 */
	public List<FetchDatum> fetchDatumList(){ return null;};
	
	public void fetchDatumList(FetchDispatch dispatch){};

	public FetchDatum fetchDatum(FetchDatum datum){return null;};

    protected RawContent fetchRawContent(Map<String,Object> metadata) {
        String url = StringUtils.valueOf(metadata.get(Constants.QUEUE_URL));
        Object method = metadata.get(Constants.QUEUE_METHOD);
        Object header = metadata.get(Constants.QUEUE_HEADER);
        if (StringUtils.isEmpty(url)) {
            throw new RuntimeException("url is empty");
        }
        url = new QueryUrl(url).toString();
        HttpPage page;
        if (method != null && "post".equalsIgnoreCase(method.toString())) {
            page = this.advancePageLoader.post((Map) header, null, url);
        } else {
            page = this.advancePageLoader.get((Map) header, url);
        }
        RawContent raw = new RawContent(url, page.getContentHtml());
        return raw;
    }


    protected void moveNext(){};

	public boolean isComplete(){return false;};
	
	/**
	 * do something when a datum is fetched finish
	 * @param datum
	 */
	public void datumFinish(FetchDatum datum){
		// this method do nothing.
		// you can add your business in your fetcher object
	}
	
	/**
	 * do something when a crawl task start
	 */
	public void taskInitialize(){
		// this method do nothing.
		// you can add your business in your fetcher object
	}
	
	/**
	 * do something when a crawl task is finish
	 */
	public void taskFinish(){
		// this method do nothing.
		// you can add your business in your fetcher object
	}

    // page downloader helpers

    public HttpPage downloadDocument(String url){
        if(org.apache.commons.lang.StringUtils.isEmpty(url)){
            return null;
        }
        HttpPage page = advancePageLoader.get(url);
        if(page.getStatusCode()!= HttpStatus.SC_OK){
            return null;
        }
        return page;
    }

    public HttpPage downloadDocument(String url, Map<String, String> header){
        if(org.apache.commons.lang.StringUtils.isEmpty(url)){
            return null;
        }
        HttpPage page = advancePageLoader.get(header, url);
        if(page.getStatusCode()!=HttpStatus.SC_OK){
            return null;
        }
        return page;
    }
}
