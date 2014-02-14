package com.sdata.common.fetcher;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.Map;

import org.jsoup.nodes.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.lakeside.core.utils.PatternUtils;
import com.lakeside.core.utils.StringUtils;
import com.lakeside.core.utils.UrlUtils;
import com.lakeside.core.utils.time.DateFormat;
import com.lakeside.core.utils.time.DateTimeUtils;
import com.sdata.common.CommonDatum;
import com.sdata.common.CommonItem;
import com.sdata.common.IDBuilder;
import com.sdata.common.queue.CommonLink;
import com.sdata.common.queue.CommonLinkQueue;
import com.sdata.common.queue.CommonQueueFactory;
import com.sdata.context.config.Configuration;
import com.sdata.context.config.Constants;
import com.sdata.context.state.RunState;
import com.sdata.core.FetchDispatch;
import com.sdata.core.exception.NegligibleException;
import com.sdata.core.parser.html.util.DocumentUtils;
import com.sdata.core.util.JsoupUtils;
import com.sdata.proxy.SenseFetchDatum;
import com.sdata.proxy.fetcher.SenseFetcher;
import com.sdata.proxy.fetcher.SenseItemMonitor;
import com.sdata.proxy.item.SenseCrawlItem;

import de.jetwick.snacktory.HtmlFetcher;
import de.jetwick.snacktory.JResult;

/**
 * common crawler for news,blog etc global site crawl
 * 
 * @author zhufb
 * 
 */
public class CommonFetcher extends SenseFetcher {

	protected static Logger log = LoggerFactory.getLogger("Common.CommonFetcher");
	public final static String FID = "common";

	public CommonFetcher(Configuration conf, RunState state) {
		super(conf, state);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.sdata.proxy.fetcher.SenseFetcher#fetchDatumList(com.sdata.core.
	 * FetchDispatch, com.sdata.proxy.item.SenseCrawlItem)
	 */
	@Override
	public void fetchDatumList(FetchDispatch fetchDispatch,
			SenseCrawlItem crawlItem) {
		CommonItem item = (CommonItem) crawlItem;
		CommonLinkQueue linkQueue = CommonQueueFactory.getLinkQueue(item);
		String init = crawlItem.parse();
		Document document = DocumentUtils.getDocument(init);
		List<String> list = JsoupUtils.getListLink(document, "a");
		linkQueue.add(this.filter(list,item), Constants.QUEUE_LEVEL_ROOT + 1);
		//Important: this type queue's item push and poll all depend fetch datum process
		// and if datum level > level limit ,fetch datum process notify 
		int total = 0; 
		while (!isComplete(item)&& total< 60) {
			try {
				CommonLink clink = linkQueue.get();
				if(clink == null){
					total+=2;
					continue;
				}
				total = 0;
				CommonDatum comLnkToDatum = this.ComLinkToDatum(item, clink);
				fetchDispatch.dispatch(comLnkToDatum);
			}catch(InterruptedException e) {
				e.printStackTrace();
			}
		}
		CommonQueueFactory.destory(item);
		log.warn(" item " + item.getId() + " completed and clear link queue!");

	}

	/**
	 * dispatch link queue
	 * @param item
	 * @param linkQueue
	 *            links queue
	 */
	protected CommonDatum ComLinkToDatum(CommonItem item,
			CommonLink clink) {
		CommonDatum datum = new CommonDatum();
		if(clink == null){
			return null;
		}
		String link = clink.getLink();
		byte[] id = IDBuilder.build(item, link.hashCode());
		datum.setUrl(link);
		datum.setId(id);
		datum.setLevel(clink.getLevel());
		datum.setCrawlItem(item);
		return datum;
	}

	@Override
	public SenseFetchDatum fetchDatum(SenseFetchDatum datum) {
		CommonDatum cdatum = (CommonDatum) datum;
		CommonItem citem = (CommonItem) datum.getCrawlItem();
		// if current level > level limit complete notify complete
		if (cdatum.getLevel() > citem.getLevelLimit()) {
			SenseItemMonitor.notify(citem);
			return null;
		}
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
			log.info("This link is one category:" + url);
			CommonLinkQueue linkQueue = CommonQueueFactory.getLinkQueue(citem);
			int level = cdatum.getLevel() + 1;
			linkQueue.add(filter(res.getLinks(), citem), level);
			return null;
		}
		log.info("This link is one article:" + url);
		Map<String, Object> data = this.filter(res.toMap());
		if(data == null){
			log.info("This link publish time is not match our need :" + url);
			return null;
		}
		cdatum.addAllMetadata(data);
		cdatum.addMetadata(com.sdata.proxy.Constants.DATA_ID, cdatum.getId());
		return cdatum;
	}
	
	/**
	 * fetch and extract this url
	 * 
	 * @param cdatum
	 * @return
	 */
	protected JResult fetchAndExtract(String url){
		return new HtmlFetcher().fetchAndExtract(url);
	}

	protected Map<String,Object> filter(Map<String, Object> data){
		Date date = DateFormat.strToDate(data.get(com.sdata.proxy.Constants.PUB_TIME));
		if(date == null){
			return null;
		}
		Date now = new Date();
		Date oneYago = DateTimeUtils.add(now,Calendar.YEAR, -1);
		if(date.before(oneYago)){
			return null;
		}
		return data;
	}
	
	/**
	 * filter the links with url pattern
	 * 
	 * @param res
	 * @param item
	 * @return
	 */
	protected List<String> filter(List<String> links, CommonItem item) {
		List<String> list = new ArrayList<String>();
		for (String link : links) {
			String url = UrlUtils.clean(link);
			if (StringUtils.isEmpty(url)) {
				continue;
			}
			if (list.contains(url)) {
				continue;
			}
			if (!UrlUtils.getHost(url).endsWith(item.getHost())) {
				continue;
			}
			String urlPattern = item.getUrlPattern();
			if (!StringUtils.isEmpty(urlPattern)
					&& !PatternUtils.matches(urlPattern, url)) {
				continue;
			}
			list.add(url);
		}
		return list;
	}

}
