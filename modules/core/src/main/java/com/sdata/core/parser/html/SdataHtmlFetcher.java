package com.sdata.core.parser.html;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.lakeside.core.utils.StringUtils;
import com.lakeside.download.http.HttpPageLoader;
import com.sdata.context.config.Configuration;
import com.sdata.context.config.Constants;
import com.sdata.context.config.CrawlAppContext;
import com.sdata.context.state.RunState;
import com.sdata.core.FetchDatum;
import com.sdata.core.RawContent;
import com.sdata.core.fetcher.SdataFetcher;
import com.sdata.core.parser.ParseResult;

/**
 * SdataHtmlFetcher fetcher implement
 * 
 * @author zhufb
 *
 */
public class SdataHtmlFetcher extends SdataFetcher{
	private static final Logger log = LoggerFactory.getLogger("SdataCrawler.SdataHtmlFetcher");
	protected static CrawlLinkQueue crawlQueue = new CrawlLinkQueue(CrawlAppContext.db);
	private static boolean complete = false;
	private Map<String, Object> current = null;

	public SdataHtmlFetcher(Configuration conf,RunState state) {
		this.setConf(conf);
		this.setRunState(state);
		this.parser = new SdataHtmlParser(conf,state);
	}

	@Override
	public List<FetchDatum> fetchDatumList() {
		List<FetchDatum> datumList = new ArrayList<FetchDatum>();
		if(current != null){
			String url = (String)current.get(Constants.QUEUE_URL);
			log.info("fetch datum list:"+url);
			RawContent rawContent = this.fetchRawContent(url);
			rawContent.addAllMeata(current);
			ParseResult parseResult = parser.parseList(rawContent);
			this.put(parseResult.getCategoryList());
			datumList = parseResult.getFetchList();
		}
		
		this.moveNext();
		return datumList;
	}

	@Override
	public FetchDatum fetchDatum(FetchDatum datum) {
		if(datum == null){
			return null;
		}
		log.info("fetch datum one:"+datum.getUrl());
		ParseResult result = parser.parseSingle(this.fetchRawContent(datum.getUrl()));
		datum.addAllMetadata(result.getMetadata());
		return datum;
	}
	
	protected RawContent fetchRawContent(String url){
		if(StringUtils.isEmpty(url)){
			return null;
		}
		String content = HttpPageLoader.getAdvancePageLoader().get(url).getContentHtml();
		if(content == null){
			return null;
		}
		RawContent raw = new RawContent(url,content);
		return raw;
	}
	
	/**
	 * move to next crawl instance
	 */
	@Override
	protected void moveNext() {
		crawlQueue.complete(current);
		current = crawlQueue.get();
	}
	
	@Override
	public void taskFinish(){
		CrawlAppContext.state.resetCurrentEntry();
	}
	
	@Override
	public boolean isComplete(){
		return complete;
	}
	
	public static void setComplete(boolean complete){
		SdataHtmlFetcher.complete = complete;
	}
	
	private void put(List<Map<String,Object>> list) {
		crawlQueue.put(list);
	}
}
