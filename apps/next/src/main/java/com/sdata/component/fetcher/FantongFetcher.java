package com.sdata.component.fetcher;

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.lakeside.core.utils.StringUtils;
import com.mongodb.MongoException;
import com.sdata.component.parser.FantongParseResult;
import com.sdata.component.parser.FantongParser;
import com.sdata.core.Configuration;
import com.sdata.core.Constants;
import com.sdata.core.CrawlAppContext;
import com.sdata.core.FetchDatum;
import com.sdata.core.RawContent;
import com.sdata.core.RunState;
import com.sdata.core.crawldb.CrawlDBQueue;
import com.sdata.core.fetcher.SdataFetcher;
import com.sdata.core.parser.ParseResult;

/**
 * fantong fetcher implement
 * 
 * @author zhufb
 *
 */
public class FantongFetcher extends SdataFetcher{
	
	private static final Logger log = LoggerFactory.getLogger("SdataCrawler.FantongFetcher");
	private String ftUrl = "http://www.fantong.com/beijing/";
	private static Queue<Map<String,Object>> queue = null;
	private Map<String, Object> curCategory = null;
	private CrawlDBQueue crawlDB;
	private int topN = Constants.FETCH_COUNT;

	public FantongFetcher(Configuration conf,RunState state) throws UnknownHostException, MongoException {
		this.setConf(conf);
		this.setRunState(state);
		crawlDB = CrawlAppContext.db;
		FantongParser ftParser = new FantongParser(conf,state);
		this.parser = ftParser;
		initFantongCategory(ftParser);
	}

	/**
	 * init category data 
	 * 
	 * @param dpParser
	 */
	private void initFantongCategory(FantongParser ftParser){
		List<Map<String, Object>> categorylist = crawlDB.queryQueue(topN);
		if(categorylist==null || categorylist.size()==0){
			String content = ((FantongParser)parser).download(ftUrl);
			categorylist = ftParser.parseTopCategoryList(content);
			appendCategoryQueue(categorylist);
		}
		queue = new LinkedList<Map<String,Object>>(categorylist);
	}

	@Override
	public List<FetchDatum> fetchDatumList() {
		if(curCategory==null&&queue!=null && queue.size()>0){
			curCategory = queue.poll();
		}
		List<FetchDatum> fetchList = new ArrayList<FetchDatum>();
		if(curCategory != null){
			String link = (String)curCategory.get(Constants.QUEUE_URL);
			log.info("fetch list ["+ getCurCategoryInfo()+"]");
			String content = ((FantongParser)parser).download(link);
			RawContent c = new RawContent(link,content);
			c.setMetadata(Constants.QUEUE_DEPTH, curCategory.get(Constants.QUEUE_DEPTH));
			FantongParseResult parseList = (FantongParseResult)parser.parseList(c);
			if(parseList != null){
				appendCategoryQueue(parseList.getNewCategoryList());
				fetchList = parseList.getFetchList();
			}
		}
		//move to next 
		moveNext();
		return fetchList;
	}

	@Override
	public FetchDatum fetchDatum(FetchDatum datum) {
		if(datum!=null && !StringUtils.isEmpty(datum.getUrl())){
			String url = datum.getUrl();
			log.info("fetching shop [ "+url+"]");
			String content = ((FantongParser)parser).download(url);
			//datum.setContent(content);
			RawContent rawContent = new RawContent(url,content);
			rawContent.addAllMeata(datum.getMetadata());
			ParseResult result = parser.parseSingle(rawContent);
			if(result != null){
				datum.setMetadata(result.getMetadata());
			}else{
				throw new RuntimeException("fetch content is empty");
			}
		}
		return datum;
	}
	
	/**
	 * move to next crawl instance
	 */
	@Override
	protected void moveNext() {
		if(curCategory!=null)
			crawlDB.updateQueueComplete(curCategory.get(Constants.QUEUE_KEY).toString());
		if(queue.size() == 0){
			synchronized (queue) {
				if(queue.size()==0){
					List<Map<String, Object>> tops = crawlDB.queryQueue(topN);
					queue.addAll(tops);
				}
			}
		}
		curCategory = queue.poll();
	}
	
	@Override
	public boolean isComplete(){
		boolean complete = false;
		if(curCategory==null&&queue.size()==0) {
			//delete queue;
			crawlDB.deleteQueue();
			//update currentFetchState to a empty String for next task
			state.updateCurrentFetchState("");
			complete = true;
		}
		return complete;
	}
	
	private void appendCategoryQueue(List<Map<String, Object>> list) {
		if(list==null) return;
		this.crawlDB.insertQueueObjects(list);
	}
	
	private String getCurCategoryInfo(){
		if(this.curCategory== null){
			return null;
		}
		StringBuilder info = new StringBuilder();
		info.append("url:"+this.curCategory.get(Constants.QUEUE_URL));
		return info.toString();
	}
}
