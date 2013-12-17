package com.sdata.live.fetcher.twitter;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.sdata.context.config.Configuration;
import com.sdata.context.state.RunState;
import com.sdata.core.FetchDispatch;
import com.sdata.core.RawContent;
import com.sdata.core.parser.ParseResult;
import com.sdata.live.parser.twitter.TwitterParser;
import com.sdata.proxy.SenseConfig;
import com.sdata.proxy.fetcher.SenseFetcher;
import com.sdata.proxy.item.SenseCrawlItem;

/**
 * @author zhufb
 *
 */
public class TwitterFetcher extends SenseFetcher {
	public final static String FID = "twitter";
	protected static final Logger log = LoggerFactory.getLogger("Sense.TwitterFetcher");
	public TwitterFetcher(Configuration conf,RunState state) {
		super(conf,state);
		super.parser = new TwitterParser(conf);
	}

	@Override
	public void fetchDatumList(FetchDispatch fetchDispatch,SenseCrawlItem crawlItem) {
		Configuration conf = SenseConfig.getConfig(crawlItem);
		String baseUrl = crawlItem.parse();
		String url = baseUrl;
		boolean end = false;
		while(!end){
			log.warn("fetch datum list:"+url);
			RawContent rc = this.getRawContent(url);
			ParseResult result = parser.parseCrawlItem(conf, rc, crawlItem);
			end = this.end(result, crawlItem);
			fetchDispatch.dispatch(result.getFetchList());
			String currentStatus = ((TwitterParser)parser).getCurrentStatus(rc);
			if(StringUtils.isEmpty(currentStatus)){
				break;
			}
			url = baseUrl.concat(currentStatus);
		}
	}
}
