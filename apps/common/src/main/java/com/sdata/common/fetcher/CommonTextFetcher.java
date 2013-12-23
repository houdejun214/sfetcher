package com.sdata.common.fetcher;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.lakeside.core.utils.PatternUtils;
import com.sdata.common.CommonItem;
import com.sdata.context.config.Configuration;
import com.sdata.context.state.RunState;
import com.sdata.core.FetchDispatch;
import com.sdata.core.RawContent;
import com.sdata.proxy.item.SenseCrawlItem;

/**
 * @author zhufb
 *
 */
public class CommonTextFetcher extends CommonJsonFetcher{
	protected static  Logger log = LoggerFactory.getLogger("Common.CommonTextFetcher");
	public final static String FID = "common-text";
	private static final String REGEX ="(\\{.*\\})";
	
	public CommonTextFetcher(Configuration conf,RunState state){
		super(conf,state);
	}
	
	@Override
	public void fetchDatumList(FetchDispatch fetchDispatch, SenseCrawlItem crawlItem) {
		CommonItem commonItem = (CommonItem)crawlItem;
		String url = commonItem.parse();
		log.warn("fetch common text link: " + url);
		RawContent rc = super.getRawContent(url);
		String matchPattern = PatternUtils.getMatchPattern(REGEX, rc.getContent(), 1);
		if(!StringUtils.isEmpty(matchPattern)){
			rc.setContent(matchPattern);
		}
		super.fetchJsonData(fetchDispatch, commonItem, rc);
	}
}
