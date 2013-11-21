package com.sdata.component.fetcher;

import java.io.File;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.regex.Pattern;

import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.lakeside.core.utils.StringUtils;
import com.mongodb.MongoException;
import com.sdata.component.parser.AmazonParseResult;
import com.sdata.component.parser.AmazonParser;
import com.sdata.core.Configuration;
import com.sdata.core.Constants;
import com.sdata.core.CrawlAppContext;
import com.sdata.core.FetchDatum;
import com.sdata.core.RawContent;
import com.sdata.core.RunState;
import com.sdata.core.crawldb.CrawlDB;
import com.sdata.core.fetcher.SdataFetcher;
import com.sdata.core.parser.ParseResult;
import com.sdata.core.util.ApplicationResourceUtils;

public class AmazonFetcher extends SdataFetcher{
	
	private static final String amazonCategoryUrl="http://www.amazon.com/gp/site-directory/ref=topnav_sad";
	
	private static final Logger log = LoggerFactory.getLogger("SdataCrawler.AmazonFetcher");
	
	private Queue<Map<String,Object>> categoires=null;
	
	private Map<String, Object> curCategory=null;

	private CrawlDB crawlDB;
	
	private Boolean curCategoryOver = false;
	private int curPageNo = 1;
	private static int maxPage = 400;
	private int topN = 10;
	private List<String> categoryFilters;

	public AmazonFetcher(Configuration conf,RunState state) throws UnknownHostException, MongoException {
		this.setConf(conf);
		this.setRunState(state);
		crawlDB = CrawlAppContext.db;
		AmazonParser amazonParser = new AmazonParser(conf,state);
		this.parser = amazonParser;
		loadCategoryFilter();
		initProductCategory(amazonParser);
	}

	@Override
	public List<FetchDatum> fetchDatumList() {
		if(curCategory==null && (categoires!=null && categoires.size()>0)){
			curCategory = categoires.poll();
		}
		if(curCategory!=null){
			String link = (String)curCategory.get(Constants.QUEUE_URL);
			log.info("fetch list ["+ getCurCategoryInfo()+"]");
			String content = ((AmazonParser)parser).download(link);
			RawContent c = new RawContent(link,content);
			c.setMetadata(Constants.QUEUE_DEPTH, curCategory.get(Constants.QUEUE_DEPTH));
			AmazonParseResult parseList = (AmazonParseResult)parser.parseList(c);
			if(!parseList.isListEmpty() && parseList.getNextUrl()!=null&&maxPage>=curPageNo){
				curCategory.put(Constants.QUEUE_URL, parseList.getNextUrl());
			} else {
				curCategoryOver = true;
			}
			if(curPageNo <= 1) {
				// the first page, add the parsed category pages.
				List<Map<String, Object>> newCategoryList = parseList.getNewCategoryList();
				appendCategoryQueue(newCategoryList,false);
			}
			List<FetchDatum> fetchList = parseList.getFetchList();
			moveNext();
			return fetchList;
		}
		return null;
	}

	@Override
	public FetchDatum fetchDatum(FetchDatum datum) {
		if(datum!=null && !StringUtils.isEmpty( datum.getUrl())){
			String url = datum.getUrl();
			log.info("fetching product: "+url);
			String content = ((AmazonParser)parser).download(url);
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
		if(curCategoryOver){
			crawlDB.updateQueueComplete(curCategory.get(Constants.QUEUE_KEY).toString());
			if(categoires.size() <= 0){
				List<Map<String, Object>> tops = crawlDB.queryQueue(topN);
				categoires.addAll(tops);
			}
			curCategory = categoires.poll();
			if(curCategory == null){
				List<Map<String, Object>> tops = crawlDB.queryQueue(topN);
				categoires.addAll(tops);
				curCategory = categoires.poll();
			}
			curPageNo = 1;
			curCategoryOver = false;
		} else {
			curPageNo++;
		}
	}
	
	@Override
	public boolean isComplete(){
		boolean complete = false;
		if(curCategory==null&&categoires.size()==0) {
			//delete queue;
			crawlDB.deleteQueue();
			//update currentFetchState to a empty String for next task
			state.updateCurrentFetchState("");
			complete = true;
		}
		return complete;
	}
	
	private void initProductCategory(AmazonParser amazonParser){
		List<Map<String, Object>> categorylist = crawlDB.queryQueue(topN);
		if(categorylist==null || categorylist.size()==0){
			String content = ((AmazonParser)parser).download(amazonCategoryUrl);
			categorylist = amazonParser.parseTopCategoryList(content);
			categorylist = appendCategoryQueue(categorylist,true);
		}
		categoires = new LinkedList<Map<String,Object>>(categorylist);
	}
	
	
	private void loadCategoryFilter(){
		String filterFile = this.getConf("filterFile");
		String file = ApplicationResourceUtils.getResourceUrl(filterFile);
		try {
			categoryFilters = FileUtils.readLines(new File(file), "utf-8");
			List<String> comments = new ArrayList<String>();
			for(String filter:categoryFilters){
				if(filter.startsWith("//") || filter.startsWith("#") ){
					comments.add(filter);
				}
			}
			for(String comment:comments){
				categoryFilters.remove(comment);
			}
			
		} catch (Exception e) {
			log.error(e.getMessage(), e);
		}
	}
	
	private List<Map<String, Object>> appendCategoryQueue(List<Map<String, Object>> list,boolean needFilter) {
		List<Map<String, Object>> newCategoryList =new ArrayList<Map<String, Object>>();
		if(needFilter){
			for(Map<String, Object> obj:list){
				String name = (String)obj.get(Constants.QUEUE_KEY);
				boolean filter = false;
				for(String regex: categoryFilters){
					boolean matches = Pattern.matches(regex, name);
					if(matches){
						filter = true;
						break;
					}
				}
				if(!filter){
					newCategoryList.add(obj);
				}
			}
		}else{
			newCategoryList = list;
		}
		this.crawlDB.insertQueueObjects(newCategoryList);
		return newCategoryList;
	}
	
	private String getCurCategoryInfo(){
		StringBuilder info = new StringBuilder();
		if(this.curCategory!= null){
			info.append("catId:"+this.curCategory.get(Constants.QUEUE_KEY)+",");
			info.append("page:"+curPageNo+",");
			info.append("url:"+this.curCategory.get(Constants.QUEUE_URL));
		}
		return info.toString();
	}
}
