package com.sdata.component.fetcher;

import it.sauronsoftware.feed4j.FeedParser;
import it.sauronsoftware.feed4j.bean.Feed;
import it.sauronsoftware.feed4j.bean.FeedItem;

import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.lakeside.core.utils.UUIDUtils;
import com.sdata.component.data.dao.NewsMgDao;
import com.sdata.core.Configuration;
import com.sdata.core.Constants;
import com.sdata.core.FetchDatum;
import com.sdata.core.RunState;
import com.sdata.core.fetcher.SdataFetcher;
import com.sdata.core.util.ApplicationResourceUtils;

public class NewsByRSSFetcher extends SdataFetcher{
	
	private static final Logger log = LoggerFactory.getLogger("SdataCrawler.NewsByRSSFetcher");
	private static final String NAME = "name";
	private static final String URL = "url";
	private static final String LANG = "lang";
	
	private List<Map<String,String>> sourceList;
	private NewsMgDao newsdao = new NewsMgDao();;
	
	public NewsByRSSFetcher(Configuration conf,RunState state) {
		this.setConf(conf);
		this.setRunState(state);
		String host = this.getConf("mongoHost");
		int port = this.getConfInt("mongoPort",27017);
		String dbName = this.getConf("mongoDbName");
		this.newsdao.initilize(host, port, dbName);
		this.initSources();
	}

	@Override
	public List<FetchDatum> fetchDatumList() {
		List<FetchDatum> resultList = new ArrayList<FetchDatum>();
		while(sourceList==null || sourceList.size()==0){
			this.initSources();
		}
		try {
			for(Map<String,String> newsSource:sourceList){
				String name = newsSource.get(NAME);
				String url = newsSource.get(URL);
				String lang = newsSource.get(LANG);
				URL url_u = null;
				Feed feed = null;
				try {
					url_u = new URL(url);
					feed = FeedParser.parse(url_u);
				} catch (MalformedURLException e) {
					e.printStackTrace();
				} catch (Exception e) {
					log.info("crawler has sth wrong when crawling name["+ name +"],url["+url+"]");
				}
				int countThisTime = 0;
				if(feed!=null && feed.getItemCount()>0){
					int items = feed.getItemCount();
					for (int i = 0; i < items; i++) {
						FetchDatum datum = new FetchDatum();
						FeedItem item = feed.getItem(i);
						String title = item.getTitle();
						String link = item.getLink().toString();
						String description = item.getDescriptionAsText();
						String pubDateTest =item.getElementValue("", "pubDate");
						Date pubDate = item.getPubDate();
						UUID uid = UUIDUtils.getMd5UUID(name+title+pubDate);
						if(newsdao.isNewsExists(uid)){
							countThisTime = i;
							break;
						}
						datum.setUrl(link);
						datum.addMetadata("title", title);
						datum.addMetadata(Constants.OBJECT_ID, uid);
						datum.addMetadata("link", link);
						datum.addMetadata("description", description);
						datum.addMetadata("pubDate", pubDate);
						datum.addMetadata(LANG, lang);
						datum.addMetadata(NAME, name);
						resultList.add(datum);
						countThisTime = i+1;
					}
				}
				log.info("crawl source["+name+"] count:["+countThisTime+"]");
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return resultList;
	}
	
	@Override
	public FetchDatum fetchDatum(FetchDatum datum) {
		return datum;
	}
	
	/**
	 * init sources for crawler news
	 * 
	 * @author qiumm
	 */
	private void initSources() {
		synchronized (this) {
			if (sourceList == null || sourceList.size() == 0) {
				if (sourceList == null) {
					sourceList = new ArrayList<Map<String, String>>();
				}
				String conf = this.getConf("newsSourceSeedFile");
				String path = ApplicationResourceUtils.getResourceUrl(conf);
				try {
					List<String> lines = FileUtils.readLines(new File(path));
					for (String line : lines) {
						String[] splits = line.split(",");
						if (splits == null || splits.length != 3) {
							throw new RuntimeException(
									"there is invalid line in newsSourceSeedFile!");
						}
						Map<String, String> meta = new HashMap<String, String>();
						meta.put(NAME, splits[0]);
						meta.put(URL, splits[1]);
						meta.put(LANG, splits[2]);
						sourceList.add(meta);
					}
				} catch (IOException e) {
					throw new RuntimeException(e);
				}
			}
		}
	}
	
	/**
	 * move to next crawl instance
	 */
	@Override
	protected void moveNext() {
	}
	
	@Override
	public boolean isComplete(){
		return false;
		
	}
}
