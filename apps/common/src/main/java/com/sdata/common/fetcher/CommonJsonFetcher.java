package com.sdata.common.fetcher;

import java.util.List;

import net.sf.json.JSONObject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.lakeside.core.utils.MapUtils;
import com.lakeside.core.utils.StringUtils;
import com.sdata.common.CommonDatum;
import com.sdata.common.CommonItem;
import com.sdata.common.queue.CommonLink;
import com.sdata.context.config.Configuration;
import com.sdata.context.config.Constants;
import com.sdata.context.state.RunState;
import com.sdata.core.FetchDispatch;
import com.sdata.core.RawContent;
import com.sdata.core.exception.NegligibleException;
import com.sdata.proxy.SenseFetchDatum;
import com.sdata.proxy.item.SenseCrawlItem;

import de.jetwick.snacktory.JResult;

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
		log.info("fetch common json link: "+url);
		RawContent rc = super.getRawContent(url);
		this.fetchJsonData(fetchDispatch, commonItem, rc);
	}
	
	/**
	 * fetch json data 
	 * 
	 * @param fetchDispatch
	 * @param item
	 * @param rc
	 */
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
		List<String> list = filter(links, item);
		boolean end = false;
		for(int i=0; !end && i<list.size(); i++){
			String link = list.get(i);
			CommonLink clink = new CommonLink(link,Constants.QUEUE_LEVEL_ROOT);
			CommonDatum datum = super.ComLinkToDatum(item, clink);
			end = super.end(datum, item);
			fetchDispatch.dispatch(datum);
		}
	}
	
	@Override
	public SenseFetchDatum fetchDatum(SenseFetchDatum datum) {
		CommonDatum cdatum = (CommonDatum) datum;
		String url = cdatum.getUrl();
		if (StringUtils.isEmpty(url)) {
			throw new NegligibleException("common datum url is null!");
		}
		// fetch and extract the url
		JResult res = this.fetchAndExtract(url);
		if (res == null) {
			return null;
		}
		// current
		else if (!res.isArticle()) {
			log.warn("This link should be an article,but extract result is not: "+url);
			return null;
		}
		log.info("This link is one article:" + url);
		cdatum.addAllMetadata(res.toMap());
		cdatum.addMetadata(com.sdata.proxy.Constants.DATA_ID, cdatum.getId());
		return cdatum;
	}
}
