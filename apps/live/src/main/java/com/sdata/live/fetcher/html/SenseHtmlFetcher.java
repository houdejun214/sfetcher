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
import com.sdata.live.LiveItem;
import com.sdata.live.parser.html.SenseHtmlParser;
import com.sdata.proxy.SenseConfig;
import com.sdata.proxy.fetcher.SenseFetcher;
import com.sdata.proxy.item.SenseCrawlItem;

/**
 * @author zhufb
 *
 */
public class SenseHtmlFetcher extends SenseFetcher{
	protected static  Logger log = LoggerFactory.getLogger("Sense.SenseHtmlFetcher");
	public final static String FID = "html";
	
	public SenseHtmlFetcher(Configuration conf,RunState state){
		super(conf,state);
		this.parser = new SenseHtmlParser(conf,state);
	}
	
	@Override
	public void fetchDatumList(FetchDispatch fetchDispatch, SenseCrawlItem crawlItem) {
			Configuration conf = SenseConfig.getConfig(crawlItem);
			int pages = conf.getInt("sense.html.page.max", Integer.MAX_VALUE);
			List<String> categoryList = new ArrayList<String>();
			categoryList.add(((LiveItem)crawlItem).parse(conf.get("sense.html.charset")));
			int current = 0;
			boolean isEnd = false;
			while(categoryList.size() > current && current < pages && !isEnd){
				String url = categoryList.get(current);
				log.warn("fetch datum list:"+url);
				RawContent rc = super.getRawContent(url);
				ParseResult result = parser.parseCrawlItem(conf,rc,crawlItem);
				isEnd = this.end(result.getFetchList(), crawlItem);
				fetchDispatch.dispatch(result.getFetchList());
				this.mergeNoRepeat(categoryList,result.getCategoryList());
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
}