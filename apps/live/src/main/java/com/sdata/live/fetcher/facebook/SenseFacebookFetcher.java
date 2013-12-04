package com.sdata.live.fetcher.facebook;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.lakeside.core.utils.StringUtils;
import com.sdata.context.config.Configuration;
import com.sdata.context.state.RunState;
import com.sdata.core.FetchDispatch;
import com.sdata.core.RawContent;
import com.sdata.core.parser.ParseResult;
import com.sdata.live.parser.facebook.SenseFacebookParser;
import com.sdata.sense.SenseConfig;
import com.sdata.sense.SenseFetchDatum;
import com.sdata.sense.fetcher.SenseFetcher;
import com.sdata.sense.item.SenseCrawlItem;

/**
 * @author zhufb
 *
 */
public class SenseFacebookFetcher extends SenseFetcher{
	private static final Logger log = LoggerFactory.getLogger("Sense.SenseFacebookFetcher");
	public final static String FID = "facebook";
	
	public SenseFacebookFetcher(Configuration conf,RunState state){
		super(conf,state);
		this.parser = new SenseFacebookParser(conf);
	}
	
	@Override
	public void fetchDatumList(FetchDispatch fetchDispatch, SenseCrawlItem crawlItem) {
		Configuration conf = SenseConfig.getConfig(crawlItem);
		String url = crawlItem.parse();
		boolean end = false;
		while(!StringUtils.isEmpty(url)&&!end){;
			url = ((SenseFacebookParser)parser).mergeTokenUrl(conf,url);
			log.warn("fetch datum list:"+ url);
			RawContent rc = super.getRawContent(url);
			ParseResult result = parser.parseCrawlItem(conf,rc,crawlItem);
			end = end(result, crawlItem);
			fetchDispatch.dispatch(result.getFetchList());
			List<String> categoryList = result.getCategoryList();
			if(categoryList!=null&&categoryList.size() >0){
				url = categoryList.get(0);
			}else{
				url = null;
			}
		}
	}

	@Override
	public SenseFetchDatum fetchDatum(SenseFetchDatum datum) {
		Configuration conf = SenseConfig.getConfig(datum.getCrawlItem());
		this.parser.parseDatum(datum, conf,null);;
		return datum;
	}
	
}
