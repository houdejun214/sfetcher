package com.sdata.component.fetcher;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.MalformedURLException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.lakeside.core.utils.StringUtils;
import com.lakeside.core.utils.UrlUtils;
import com.lakeside.core.utils.time.StopWatch;
import com.mongodb.MongoException;
import com.sdata.component.data.storer.FashionBlogSqlStorer;
import com.sdata.component.filter.FashionBlogsFilter;
import com.sdata.component.parser.YoutubeParser;
import com.sdata.core.Configuration;
import com.sdata.core.Constants;
import com.sdata.core.CrawlAppContext;
import com.sdata.core.FetchDatum;
import com.sdata.core.QueueStatus;
import com.sdata.core.RunState;
import com.sdata.core.crawldb.CrawlDBQueue;
import com.sdata.core.fetcher.SdataFetcher;
import com.sdata.core.parser.html.util.DocumentUtils;
import com.sdata.core.util.ApplicationResourceUtils;
import com.sdata.core.util.WebPageDownloader;

/**
 * fetch youtube video
 * 
 * @author qmm
 *
 */
public class FashionBlogsFetcher extends SdataFetcher{
	
	private static final Logger log = LoggerFactory.getLogger("SdataCrawler.FashionBlogsFetcher");
	
	private List<String> BlogsList = new ArrayList<String>();
	private String BlogsListFile;
	
	private BlockingQueue<Map<String,Object>> queue;
	private CrawlDBQueue crawlDB;
	private FashionBlogSqlStorer resultStorer;
	private FashionBlogsFilter urlFilter;
	
	private int topN = Constants.FETCH_COUNT;
	
	private String currentFetchUrl;
	
	private boolean isCompelet;
	
	private HashSet<String> checkRepeatSet;
	
	private String urlCompare;
	
	private int maxCrawlDBQueueSize;
	
	public FashionBlogsFetcher(Configuration conf,RunState state) throws UnknownHostException, MongoException {
		this.setConf(conf);
		this.setRunState(state);
		this.parser = new YoutubeParser(conf,state);
		crawlDB = CrawlAppContext.db;
		resultStorer = new FashionBlogSqlStorer(conf);
		urlFilter = new FashionBlogsFilter(conf);
		this.state = state;
		BlogsListFile = this.getConf("BlogsListFile");
		maxCrawlDBQueueSize = conf.getInt("maxCrawlDBQueueSize", 100000);
		queue = new LinkedBlockingQueue<Map<String,Object>>(1000);
		currentFetchUrl = state.getCurrentFetchState();
		isCompelet = false;
		checkRepeatSet = new HashSet<String>();
		getUrlCompare(false);
	}

	private void getUrlCompare(boolean dealUrl) {
		if(StringUtils.isNotEmpty(currentFetchUrl)){
			Document docCompare = this.downloadPage(currentFetchUrl);
			if(docCompare==null){
				crawlDB.updateQueueStatus(currentFetchUrl, QueueStatus.COMPLETE);
				urlCompare = null;
				return;
			}
			urlCompare = docCompare.baseUri();
			urlCompare = urlCompare.replace("http://", "");
			urlCompare = urlCompare.replace("www.", "");
			log.info("the home page compare url is: "+urlCompare);
			if(dealUrl){
				//非post页面要爬取页面中的所有link
				String depth = "1";
				findUrlToCrawlDB(docCompare,depth);
			}
		}else{
			urlCompare = null;
		}
		
	}
	
	/**
	 * 
	 * initialize the task 
	 * 
	 */
	@Override
	public void taskInitialize() {
		// load the id list;
		String path = ApplicationResourceUtils.getResourceUrl(BlogsListFile);
		BufferedReader reader = null;
		try {
			reader = new BufferedReader(new InputStreamReader(new FileInputStream(path), "utf-8"));
			String line = null;
			boolean passCurrent = false;
			while ((line = reader.readLine()) != null) {
				if (StringUtils.isNotEmpty(line)) {
					if(StringUtils.isNotEmpty(currentFetchUrl) && !passCurrent){
						if(line.trim().equals(currentFetchUrl)){
							passCurrent = true;
						}
						continue;
					}else{
						BlogsList.add(line.trim());
					}
					
				}
			}
		} catch (Exception e) {
			throw new RuntimeException(e);
		} finally {
			try {
				if (reader != null) {
					reader.close();
				}
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		crawlDB.changeQueueStatus(QueueStatus.RUNING, QueueStatus.INIT);
	}
	
	private void getNextFetchList(){
		synchronized(BlogsList){
			int waitTime = 0;
			while(this.queue.size()==0){
				List<Map<String, Object>> nextList = null;
				boolean needQueryQueue = true;
				if(StringUtils.isNotEmpty(currentFetchUrl)){
					//查看当前种子url是否已经爬取到post
					Boolean checkKeyHaveSuccess = resultStorer.checkKeyHaveSuccess(currentFetchUrl, "1");
					if(!checkKeyHaveSuccess){
						//如果没有爬取到psot，检查depth为1的url是否爬取完毕
						Boolean queueDepthComplete = crawlDB.isQueueDepthComplete("1");
						if(queueDepthComplete){
							//当depth为1的url都爬取完毕了，仍旧没有爬取到post，则认为本crawl无法爬取此种子url，放弃此url
							needQueryQueue = false;
							log.info("waittime:"+waitTime+".I am so sorry,our crawler cannot fetch this url:"+currentFetchUrl);
						}
					}else{
						//检查crawlDBQueue是否已经过大，如果数量过多，可以考虑放弃。
						int queryQueueTotoalCount = crawlDB.queryQueueTotoalCount();
						if(queryQueueTotoalCount>maxCrawlDBQueueSize){
							needQueryQueue = false;
							resultStorer.saveResult(currentFetchUrl, "2");
							log.info("waittime:"+waitTime+".too many url need to crawl,give up this feed url:"+currentFetchUrl);
						}
					}
				}
				if(needQueryQueue){
					nextList = crawlDB.queryQueue(topN);
				}else{
					nextList = new ArrayList<Map<String, Object>>();
				}
				if(nextList.size()==0){
					//当没有状态为0的url时，取状态为1(正在处理的)的url
					List<Map<String, Object>> queryQueueByStatus = crawlDB.queryQueueByStatus(topN,QueueStatus.RUNING);
					if(queryQueueByStatus.size()==0 || waitTime>=10){
						//如果0和1的状态都没有，表示此blog已经处理完毕，从txt中获取下一个blog地址
						crawlDB.deleteQueue();
						checkRepeatSet.clear();
						if(BlogsList==null || BlogsList.size()==0){
							isCompelet = true;
							return;
						}
						if(StringUtils.isNotEmpty(currentFetchUrl)){
							log.info("Congratulations, we have fetched url: "+currentFetchUrl);
						}
						currentFetchUrl = BlogsList.remove(0);
						Map<String,Object> object = new HashMap<String,Object>();
						object.put(Constants.QUEUE_KEY, currentFetchUrl);
						object.put(Constants.QUEUE_NAME, currentFetchUrl);
						object.put(Constants.QUEUE_URL, currentFetchUrl);
						object.put(Constants.QUEUE_DEPTH, "0");
						List<Map<String,Object>> objectList = new ArrayList<Map<String,Object>>();
						objectList.add(object);
						crawlDB.insertQueueObjects(objectList);
						state.updateCurrentFetchState(currentFetchUrl);
						resultStorer.saveResult(currentFetchUrl, "0");
						log.info("All right, we are goint to fetch url: "+currentFetchUrl);
						getUrlCompare(true);
						crawlDB.updateQueueStatus(currentFetchUrl, QueueStatus.COMPLETE);
					}else{
						//如果有1状态的url，表示其他线程正在处理中，可能会产生新的0状态的url
						waitTime++;
						this.await(10000);
					}
				}else{
					waitTime = 0;
					//获取状态为0的url，修改其状态为1，表示正在处理中
					for(Map<String, Object> next:nextList){
						String key =StringUtils.valueOf(next.get(Constants.QUEUE_KEY));
						crawlDB.updateQueueStatus(key, QueueStatus.RUNING);
					}
					queue.addAll(nextList);
				}
			}
		}
	}
	
	/**
	 * this method will be call with multiple-thread
	 * @throws  
	 * 
	 */
	@Override
	public List<FetchDatum> fetchDatumList() {
			StopWatch watch = new StopWatch().start();
			Map<String,Object> onePage = this.queue.poll();
			while(onePage==null){
				if(isCompelet){
					return null;
				}
				this.getNextFetchList();
				onePage = this.queue.poll();
			}
			String pageUrl = StringUtils.valueOf(onePage.get(Constants.QUEUE_URL));
			String key = StringUtils.valueOf(onePage.get(Constants.QUEUE_KEY));
			String depth = StringUtils.valueOf(onePage.get(Constants.QUEUE_DEPTH));
			try {
				if(urlFilter.filter(pageUrl)){
					log.info("because url filter by rules,give up url:"+pageUrl);
					crawlDB.updateQueueStatus(key, QueueStatus.COMPLETE);
					return null;
				}
//				String compare = pageUrl;
//				compare = compare.replace("http://", "");
//				compare = compare.replace("www.", "");
//				if(urlCompare!=null && !compare.startsWith(urlCompare)){
//					log.info("because url dont startwith urlCompare,give up url:"+pageUrl);
//					crawlDB.updateQueueStatus(key, CrawlQueueStatus.COMPLETE);
//					return null;
//				}
				long getUrlTime=watch.getElapsedTime();watch.reset();
				Document doc = downloadPage(pageUrl);
				if(doc == null){
					crawlDB.updateQueueStatus(key, QueueStatus.COMPLETE);
					return null;
				}
				List<FetchDatum> resultList = new ArrayList<FetchDatum>();
				long downloadTime=watch.getElapsedTime();watch.reset();
				//从页面中获取post，如果能取到，说明是一个post页面
				Element post = this.extractPost(doc);
				if(post!=null){
					//post页面，将返回给子线程。同时不再进行link抓取。
					FetchDatum datum = new FetchDatum();
					datum.addMetadata("doc", doc);
					datum.addMetadata("pageUrl", pageUrl);
					datum.addMetadata("feedBlogUrl", currentFetchUrl);
					resultList.add(datum);
					resultStorer.saveResult(currentFetchUrl, "1");
				}else{
					//非post页面要爬取页面中的所有link
					String childDepth = "2";
					if("0".equals(depth)){
						childDepth = "1";
					}
					findUrlToCrawlDB(doc,childDepth);
				}
				crawlDB.updateQueueStatus(key, QueueStatus.COMPLETE);
				log.info("fetch url ["+pageUrl+"] get："+getUrlTime+"ms download: "+downloadTime+"ms process:"+watch.getElapsedTime()+"ms");
				return resultList;
			} catch (Exception e) {
				crawlDB.updateQueueStatus(key, QueueStatus.COMPLETE);
				return null;
			}
	}

	private void findUrlToCrawlDB(Document doc,String depth) {
		String realBlogUrl = doc.baseUri();
		Elements as = doc.select("a");
		Iterator<Element> iterator = as.iterator();
		List<Map<String,Object>> objectList = new ArrayList<Map<String,Object>>();
		while(iterator.hasNext()){
			Element a = iterator.next();
			//获取是否是标题上的链接
			boolean isTitleUrl = getIsTitleUrl(a);
			String pageUrl =a.absUrl("href");
			int x = pageUrl.indexOf("#");
			if(x>0){
				pageUrl = pageUrl.substring(0, x);
			}
			if(!this.checkUrlRelation(realBlogUrl, pageUrl, isTitleUrl) || urlFilter.filter(pageUrl)){
				continue;
			}else{
				Map<String,Object> object = new HashMap<String,Object>();
				object.put(Constants.QUEUE_KEY, pageUrl);
				object.put(Constants.QUEUE_NAME, currentFetchUrl);
				object.put(Constants.QUEUE_URL, pageUrl);
				object.put(Constants.QUEUE_DEPTH, depth);
				if(!this.checkRepeat(pageUrl)){
					objectList.add(object);
				}
				//如果页面url是以“/page/123”结尾，则手工增加50个page，以免后期仅剩下这些页面时，爬行速度慢。
				findMorePageUrl(objectList, pageUrl,depth);
			}
		}
		crawlDB.insertQueueObjects(objectList);
	}

	private boolean getIsTitleUrl(Element a) {
		boolean isTitleUrl = false;
		Element parent = a.parent();
		if(parent!=null){
			String parentTagName = parent.tagName();
			if("h1".equals(parentTagName) || "h1".equals(parentTagName) || "h3".equals(parentTagName)){
				isTitleUrl = true;
			}
		}
		Elements children = a.children();
		if(children!=null){
			Element child = children.first();
			if(child!=null){
				String childTagName = child.tagName();
				if("h1".equals(childTagName) || "h1".equals(childTagName) || "h3".equals(childTagName)){
					isTitleUrl = true;
				}
			}
		}
		return isTitleUrl;
	}

	private boolean checkRepeat(String pageUrl) {
		if(checkRepeatSet.contains(pageUrl)){
			return true;
		}
		checkRepeatSet.add(pageUrl);
		return false;
	}

	private Document downloadPage(String blogUrl) {
		Document doc;
		try {
			WebPageDownloader downloader = new WebPageDownloader(blogUrl);
			String content = downloader.fastDownload();
			String pageUrl = downloader.getPageUrl();
			doc = DocumentUtils.parseDocument(content,pageUrl);
		} catch (Exception e) {
			return null;
		}
		return doc;
	}

	private boolean checkUrlRelation(String baseUrl,String checkUrl, boolean isTitleUrl){
		boolean isOk = false;
		String baseDomain;
		try {
			if(baseUrl.equals(checkUrl)){
				return false;
			}
			//标题上的链接不校验startwith匹配
			if(!isTitleUrl){
				String compare = checkUrl;
				compare = compare.replace("http://", "");
				compare = compare.replace("www.", "");
				if(urlCompare!=null && !compare.startsWith(urlCompare)){
					return false;
				}
			}
			baseDomain = UrlUtils.getDomainName(baseUrl);
			String checkDomain = UrlUtils.getDomainName(checkUrl);
			if(baseDomain.equals(checkDomain)){
				isOk = true;
			}
		} catch (MalformedURLException e) {
			return false;
		} catch (Exception e) {
			return false;
		}
		return isOk;
	}

	@Override
	public FetchDatum fetchDatum(FetchDatum datum) {
		Map<String, Object> metadata = datum.getMetadata();
		Document doc = (Document)metadata.get("doc");
		Element post = this.extractPost(doc);
		String title = extractTitle(post);
		if(title==null){
			title = extractTitle(doc);
		}
		String date = extractDate(doc);
		String contentText = null;
		Elements contentElement = extractContentElement(doc);
		if(contentElement!=null){
			contentText = contentElement.text();
		}
		List<String> imagesList = extractImageList(post);
		metadata.put("title", title);
		metadata.put("cText", contentText);
		metadata.put("imgs", imagesList);
		metadata.put("date", date);
		return datum;
	}
	
	/**
	 * 如果页面url是以“/page/123”结尾，则手工增加50个page，以免后期仅剩下这些页面时，爬行速度慢。
	 * @param objectList
	 * @param pageUrl
	 */
	private void findMorePageUrl(List<Map<String, Object>> objectList,
			String pageUrl,String depth) {
		Map<String, Object> object;
		boolean isEndWithSlash = false;
		if(pageUrl.endsWith("/")){
			pageUrl = pageUrl.substring(0, pageUrl.length()-1);
			isEndWithSlash = true;
		}
		Pattern blogUrlPattern = Pattern.compile("/page/[0-9]*$",Pattern.CASE_INSENSITIVE);
		Matcher matcher = blogUrlPattern.matcher(pageUrl);
		if(matcher.find()){
			String[] splits = pageUrl.split("/");
			int num = Integer.valueOf(splits[splits.length-1]);
			if((num==2 || num%50==0) && num<2000){
				for(int i=1;i<=50;i++){
					int newNum = num+i;
					String addPageNumUrl = matcher.replaceAll("/page/"+newNum);
					if(isEndWithSlash){
						addPageNumUrl+="/";
					}
					object = new HashMap<String,Object>();
					object.put(Constants.QUEUE_KEY, addPageNumUrl);
					object.put(Constants.QUEUE_NAME, currentFetchUrl);
					object.put(Constants.QUEUE_URL, addPageNumUrl);
					object.put(Constants.QUEUE_DEPTH, depth);
					if(!this.checkRepeat(addPageNumUrl)){
						objectList.add(object);
					}
				}
			}
		}
		blogUrlPattern = Pattern.compile("p=[0-9]*$",Pattern.CASE_INSENSITIVE);
		matcher = blogUrlPattern.matcher(pageUrl);
		if(matcher.find()){
			String[] splits = pageUrl.split("=");
			int num = Integer.valueOf(splits[splits.length-1]);
			if((num==2 || num%50==0) && num<2000){
				for(int i=1;i<=50;i++){
					int newNum = num+i;
					String addPageNumUrl = matcher.replaceAll("p="+newNum);
					if(isEndWithSlash){
						addPageNumUrl+="/";
					}
					object = new HashMap<String,Object>();
					object.put(Constants.QUEUE_KEY, addPageNumUrl);
					object.put(Constants.QUEUE_NAME, currentFetchUrl);
					object.put(Constants.QUEUE_URL, addPageNumUrl);
					object.put(Constants.QUEUE_DEPTH, "0");
					if(!this.checkRepeat(addPageNumUrl)){
						objectList.add(object);
					}
				}
			}
		}
	}
	
	private Element extractPost(Document doc){
		Elements postElement = doc.select(".entry");
		if(postElement==null || postElement.size()==0){
			postElement = doc.select(".post");
		}
		if(postElement==null || postElement.size()==0){
			postElement = doc.select(".article");
		}
		if(postElement==null || postElement.size()==0){
			postElement = doc.select(".postItem");
		}
		if(postElement==null || postElement.size()==0){
			postElement = doc.select(".blog-posts");
		}
		if(postElement==null || postElement.size()==0){
			postElement = doc.select(".entrybody");
		}
		if(postElement==null || postElement.size()==0){
			postElement = doc.select(".journal-entry");
		}
		if(postElement==null || postElement.size()==0){
			postElement = doc.select("article");
		}
		if(postElement==null || postElement.size()==0 || postElement.size()>1){
			return null;
		}
		return postElement.first();
	}

	private List<String> extractImageList(Element contentElement) {
		List<String> imagesList = null;
		try {
			imagesList = new ArrayList<String>();
			if(contentElement!=null){
				Elements alist = contentElement.select("a");
				if(alist!=null && alist.size()>0){
					Iterator<Element> iterator = alist.iterator();
					while(iterator.hasNext()){
						Element next = iterator.next();
						String href = next.attr("href");
						if(href.endsWith(".jpg") || href.endsWith(".jpeg") || href.endsWith(".png") || href.endsWith(".gif")){
							if(!imagesList.contains(href)){
								imagesList.add(href);
							}
						}
					}
				}
				Elements imglist = contentElement.select("img");
				if(imglist!=null && imglist.size()>0){
					Iterator<Element> iterator = imglist.iterator();
					while(iterator.hasNext()){
						Element next = iterator.next();
						String src = next.attr("src");
						if(!imagesList.contains(src)){
							imagesList.add(src);
						}
					}
				}
			}
		} catch (Exception e) {
			return null;
		}
		return imagesList;
	}

	private Elements extractContentElement(Document doc) {
		Elements contentElement = doc.select(".post .post-body");
		if(contentElement==null || contentElement.size()==0){
			contentElement = doc.select(".post .entry-content");
		}
		if(contentElement==null || contentElement.size()==0){
			contentElement = doc.select(".post .entry");
		}
		if(contentElement==null || contentElement.size()==0){
			contentElement = doc.select(".post .post-content");
		}
		if(contentElement==null || contentElement.size()==0){
			contentElement = doc.select(".post .article-content");
		}
		if(contentElement==null || contentElement.size()==0){
			contentElement = doc.select(".entry-content");
		}
		if(contentElement==null || contentElement.size()==0){
			contentElement = doc.select(".entry");
		}
		if(contentElement==null || contentElement.size()==0){
			contentElement = doc.select(".post-content");
		}
		if(contentElement==null || contentElement.size()==0){
			contentElement = doc.select(".article-content");
		}
		if(contentElement==null || contentElement.size()==0){
			contentElement = doc.select(".post");
		}
		if(contentElement==null || contentElement.size()==0){
			contentElement = doc.select(".entry");
		}
		if(contentElement==null || contentElement.size()==0){
			contentElement = doc.select(".journal-entry");
		}
		if(contentElement==null || contentElement.size()==0){
			contentElement = doc.select(".blog-posts");
		}
		if(contentElement==null || contentElement.size()==0){
			contentElement = doc.select(".entrybody");
		}
		if(contentElement==null || contentElement.size()==0){
			contentElement = doc.select("article");
		}
		return contentElement;
	}

	private String extractTitle(Element doc) {
		String title = null;
		try {
			Elements titleElement = doc.select(".entry-title");
			if(titleElement==null || titleElement.size()==0){
				titleElement = doc.select(".post-header");
			}
			if(titleElement==null || titleElement.size()==0){
				titleElement = doc.select("h1");
			}
			if(titleElement==null || titleElement.size()==0){
				titleElement = doc.select("h2");
			}
			if(titleElement==null || titleElement.size()==0){
				titleElement = doc.select("h3");
			}
			if(titleElement!=null){
				title = titleElement.first().text();
			}
		} catch (Exception e) {
			return title;
		}
		return title;
	}
	
	private String extractDate(Element doc) {
		String date = null;
		try {
			Elements dateElement = doc.select(".date-header");
			if(dateElement==null || dateElement.size()==0){
				dateElement = doc.select(".entry-date");
			}
			if(dateElement==null || dateElement.size()==0){
				dateElement = doc.select(".article-date");
			}
			if(dateElement==null || dateElement.size()==0){
				dateElement = doc.select(".date_container");
			}
			if(dateElement==null || dateElement.size()==0){
				dateElement = doc.select(".date-post");
			}
			if(dateElement==null || dateElement.size()==0){
				dateElement = doc.select(".dateinfo");
			}
			if(dateElement==null || dateElement.size()==0){
				dateElement = doc.select(".date-info");
			}
			if(dateElement==null || dateElement.size()==0){
				dateElement = doc.select(".date");
			}
			if(dateElement==null || dateElement.size()==0){
				dateElement = doc.select(".posted-on");
			}
			if(dateElement==null || dateElement.size()==0){
				dateElement = doc.select(".entry-meta");
			}
			if(dateElement==null || dateElement.size()==0){
				dateElement = doc.select(".entrymeta");
			}
			if(dateElement==null || dateElement.size()==0){
				dateElement = doc.select(".metaDataDate");
			}
			if(dateElement==null || dateElement.size()==0){
				dateElement = doc.select(".postmetadata");
			}
			if(dateElement==null || dateElement.size()==0){
				dateElement = doc.select(".under-title span");
			}
			if(dateElement==null || dateElement.size()==0){
				dateElement = doc.select(".post_info");
			}
			if(dateElement==null || dateElement.size()==0){
				dateElement = doc.select(".authormeta");
			}
			if(dateElement!=null && dateElement.size()>0){
				date = dateElement.text();
			}
		} catch (Exception e) {
			return date;
		}
		return date;
	}

	@Override
	public boolean isComplete(){
		return isCompelet;
	}
}
