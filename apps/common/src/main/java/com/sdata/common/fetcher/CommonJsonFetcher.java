package com.sdata.common.fetcher;

import java.util.List;

import net.sf.json.JSONObject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.lakeside.core.utils.MapUtils;
import com.lakeside.core.utils.StringUtils;
import com.sdata.common.CommonItem;
import com.sdata.common.queue.CommonLinkQueue;
import com.sdata.common.queue.CommonQueueFactory;
import com.sdata.context.config.Configuration;
import com.sdata.context.config.Constants;
import com.sdata.context.state.RunState;
import com.sdata.core.FetchDispatch;
import com.sdata.core.RawContent;
import com.sdata.core.exception.NegligibleException;
import com.sdata.proxy.item.SenseCrawlItem;

/**
 * @author zhufb
 *
 */
public class CommonJsonFetcher extends CommonFetcher{
	protected static  Logger log = LoggerFactory.getLogger("Common.CommonJsonFetcher");
	protected static String HTTP_PATTERN = "(http|ftp|https):\\/\\/[\\w\\-_]+(\\.[\\w\\-_]+)+([\\w\\-\\.,@?^=%&amp;:/~\\+#]*[\\w\\-\\@?^=%&amp;/~\\+#])?";
	public final static String FID = "common-json";
	
	public CommonJsonFetcher(Configuration conf,RunState state){
		super(conf,state);
	}
	
	@Override
	public void fetchDatumList(FetchDispatch fetchDispatch, SenseCrawlItem crawlItem) {
		CommonItem commonItem = (CommonItem)crawlItem;
		String url = crawlItem.parse();
		log.warn("fetch common json link: "+url);
		RawContent rc = super.getRawContent(url);
		this.fetchJsonData(fetchDispatch, commonItem, rc);
	}
	
	protected void fetchJsonData(FetchDispatch fetchDispatch,CommonItem item,RawContent rc){
		String content = rc.getContent();
		if(StringUtils.isEmpty(content)){
			throw new NegligibleException("common json content is empty, url "+ rc.getUrl());
		}
		// if not JSON format it's html document 
		if(!(content.startsWith("{")&&content.endsWith("}"))){
			throw new NegligibleException("common json content is not json format, url "+ rc.getUrl());
		}
		JSONObject json = JSONObject.fromObject(content);
		List<String> links = MapUtils.getListPattern(json, HTTP_PATTERN);
		CommonLinkQueue linkQueue = CommonQueueFactory.getLinkQueue(item);
		linkQueue.add(filter(links, item), Constants.QUEUE_LEVEL_ROOT);
		super.dispatchLinkQueue(fetchDispatch, item, linkQueue);
	}
}
