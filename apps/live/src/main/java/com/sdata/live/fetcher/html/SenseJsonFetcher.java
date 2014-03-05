package com.sdata.live.fetcher.html;

import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.sdata.context.config.Configuration;
import com.sdata.context.state.RunState;
import com.sdata.core.FetchDispatch;
import com.sdata.core.RawContent;
import com.sdata.core.parser.ParseResult;
import com.sdata.live.parser.html.SenseJsonParser;
import com.sdata.proxy.SenseConfig;
import com.sdata.proxy.item.SenseCrawlItem;

/**
 * @author zhufb
 *
 */
public class SenseJsonFetcher extends SenseHtmlFetcher{
	protected static  Logger log = LoggerFactory.getLogger("Sense.SenseJsonFetcher");
	public final static String FID = "json";
	
	public SenseJsonFetcher(Configuration conf,RunState state){
		super(conf,state);
		this.parser = new SenseJsonParser(conf,state);
	}
	
	@Override
	public void fetchDatumList(FetchDispatch fetchDispatch, SenseCrawlItem crawlItem) {
		Configuration conf = SenseConfig.getConfig(crawlItem);
		List<String> categoryList = new ArrayList<String>();
		categoryList.add(crawlItem.parse());
		int current = 0;
		while(categoryList.size() > current){
			String url = categoryList.get(current);
			log.warn("fetch datum list:"+url);
			RawContent rc = super.getRawContent(url);
			ParseResult result = parser.parseCrawlItem(conf,rc,crawlItem);
			fetchDispatch.dispatch(result.getFetchList());
			super.mergeNoRepeat(categoryList,result.getCategoryList());
			current++;
		}
	}
}
