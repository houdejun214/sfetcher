package com.sdata.common.fetcher;

import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.lakeside.core.utils.PatternUtils;
import com.lakeside.core.utils.StringUtils;
import com.sdata.common.CommonItem;
import com.sdata.common.parser.CommonParser;
import com.sdata.context.config.Configuration;
import com.sdata.context.state.RunState;
import com.sdata.core.FetchDatum;
import com.sdata.core.FetchDispatch;
import com.sdata.core.RawContent;
import com.sdata.core.parser.ParseResult;
import com.sdata.proxy.SenseConfig;
import com.sdata.proxy.SenseFetchDatum;
import com.sdata.proxy.fetcher.SenseFetcher;
import com.sdata.proxy.item.SenseCrawlItem;

import de.jetwick.snacktory.JResult;

/**
 *  common crawler for news,blog etc global site crawl 
 * 
 * @author zhufb
 *
 */
public class CommonFetcher extends SenseFetcher {
	
	protected static  Logger log = LoggerFactory.getLogger("Common.CommonFetcher");
	public final static String FID = "common";
	
	public CommonFetcher(Configuration conf, RunState state) {
		super(conf, state);
		this.parser = new CommonParser(conf,state);
	}

	/* (non-Javadoc)
	 * @see com.sdata.proxy.fetcher.SenseFetcher#fetchDatumList(com.sdata.core.FetchDispatch, com.sdata.proxy.item.SenseCrawlItem)
	 */
	@Override
	public void fetchDatumList(FetchDispatch fetchDispatch, SenseCrawlItem crawlItem) {
		List<String> linkQueue = new ArrayList<String>();
		linkQueue.add(((CommonItem)crawlItem).parse());
		this.fetchLinks(fetchDispatch, crawlItem, linkQueue);
	}
	
	/**
	 * fetch link list 
	 * 
	 * @param fetchDispatch
	 * @param item      
	 * @param links          first link list to fetch
	 */
	protected void fetchLinks(FetchDispatch fetchDispatch,SenseCrawlItem item,List<String> links){
		Configuration conf = SenseConfig.getConfig(item);
		int current = 0;
		boolean end = false;
		while(links.size() > current && !end){
			String url = links.get(current);
			log.warn("fetch common link:"+ url);
			RawContent rc = new RawContent(url,null);
			ParseResult result = parser.parseCrawlItem(conf,rc,item);
			List<FetchDatum> fetchList = result.getFetchList();
			// when fetch datum is not null ,category is null and else
			if(fetchList!=null&&fetchList.size()>0){
				end = this.end(result, item);
				fetchDispatch.dispatch(fetchList);
			}else{
				this.mergeNoRepeat(links,filter(result.getCategoryList(),item));
			}
			current++;
		}
	}

	protected void mergeNoRepeat(List<String> dest,List<String> src){
		for(String s:src){
			if(!dest.contains(s)){
				dest.add(s);
			}
		}
	}
	
	/**
	 * filte the links with url pattern
	 * 
	 * @param res
	 * @param item
	 * @return
	 */
	protected List<String> filter(List<String> links,SenseCrawlItem item) {
		List<String> list = new ArrayList<String>();
		for(String link: links){
			String trimLink = StringUtils.trim(link);
			if(trimLink!=null&&!list.contains(trimLink)&&PatternUtils.matches(((CommonItem)item).getUrlPattern(), trimLink))
				list.add(trimLink);
		}
		return list;
	}
	
	@Override
	public SenseFetchDatum fetchDatum(SenseFetchDatum datum){
		return datum;
	}

}
