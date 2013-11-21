package com.sdata.core;

import java.io.Serializable;

import com.sdata.core.http.HttpPageLoader;



public class Test implements Serializable{

	/**
	 * 
	 */
	private static final long serialVersionUID = 8725255601972539903L;

	public static void main(String[] objs) throws Exception{

		
//		Map<String,String> header = new  HashMap<String,String>();
//    	header.put("X-Requested-With", "XMLHttpRequest");
//		String url = "http://huati.weibo.com/aj_topic/list?all=1&pic=0&hasv=0&atten=0&prov=0&city=0&_t=0&order=hot&p=1&keyword=%E5%8F%B2%E7%8E%89%E6%9F%B1%E9%80%80%E4%BC%91";
//		String content = WebPageDownloader.download(url, header);
//		System.out.println(content);
		
		
		HttpPageLoader defaultPageLoader = HttpPageLoader.getDefaultPageLoader();
//    	HttpPage page = defaultPageLoader.download(header,url);
//    	String contentHtml = page.getContentHtml();
//    	System.out.println(contentHtml);
    	
    	
//		HttpParams params = new BasicHttpParams();
//		HttpProtocolParamBean paramsBean = new HttpProtocolParamBean(params);
//		paramsBean.setVersion(HttpVersion.HTTP_1_1);
//		paramsBean.setContentCharset("UTF-8");
//		paramsBean.setUseExpectContinue(false);
//		params.setParameter(CoreProtocolPNames.USER_AGENT, "crawler4j");
//		params.setIntParameter(CoreConnectionPNames.SO_TIMEOUT, 20000);
//		params.setIntParameter(CoreConnectionPNames.CONNECTION_TIMEOUT, 30000);
//
//		params.setBooleanParameter("http.protocol.handle-redirects", false);
//
//		SchemeRegistry schemeRegistry = new SchemeRegistry();
//		schemeRegistry.register(new Scheme("http", 80, PlainSocketFactory.getSocketFactory()));
//		schemeRegistry.register(new Scheme("https", 443, SSLSocketFactory.getSocketFactory()));
//
//		ThreadSafeClientConnManager connectionManager = new ThreadSafeClientConnManager(schemeRegistry);
//		connectionManager.setMaxTotal(100);
//		connectionManager.setDefaultMaxPerRoute(100);
//		DefaultHttpClient client = new DefaultHttpClient(connectionManager, params);
//		client.addResponseInterceptor(new HttpResponseInterceptor() {
//	            public void process(final HttpResponse response, final HttpContext context) throws HttpException,
//	                    IOException {
//	                HttpEntity entity = response.getEntity();
//	                Header contentEncoding = entity.getContentEncoding();
//	                if (contentEncoding != null) {
//	                    HeaderElement[] codecs = contentEncoding.getElements();
//	                    for (HeaderElement codec : codecs) {
//	                        if (codec.getName().equalsIgnoreCase("gzip")) {
//	                            response.setEntity(new GzipDecompressingEntity(response.getEntity()));
//	                            return;
//	                        }
//	                    }
//	                }
//	            }
//
//	        });
//		
//		IdleConnectionMonitorThread connectionMonitorThread = new IdleConnectionMonitorThread(connectionManager);
//		connectionMonitorThread.start();
//////		GetMethod get = new GetMethod();
//		HttpGet get  = new HttpGet("http://blog.ifeng.com/article/25621337.html");
//		get.addHeader("Accept-Encoding", "gzip");
//		HttpResponse response = client.execute(get);
//		byte[] contentData = EntityUtils.toByteArray(response.getEntity());
//		String string = new String(contentData, "UTF-8");
//		System.out.println(string);
//		
////		
//    	String crawlStorageFolder = "/data/crawl/root";
//        int numberOfCrawlers = 1;
//
//        CrawlConfig config = new CrawlConfig();
//        config.setCrawlStorageFolder(crawlStorageFolder);
//
//        /*
//         * Instantiate the controller for this crawl.
//         */
//        PageFetcher pageFetcher = new PageFetcher(config);
//        RobotstxtConfig robotstxtConfig = new RobotstxtConfig();
//        RobotstxtServer robotstxtServer = new RobotstxtServer(robotstxtConfig, pageFetcher);
//        CrawlController controller = new CrawlController(config, pageFetcher, robotstxtServer);
//
//        /*
//         * For each crawl, you need to add some seed urls. These are the first
//         * URLs that are fetched and then the crawler starts following links
//         * which are found in these pages
//         */
//        controller.addSeed("http://blog.ifeng.com/article/25621337.html");
////        controller.addSeed("http://news.ifeng.com/taiwan/3/detail_2013_04/11/24108063_0.shtml");
////        controller.addSeed("http://www.ics.uci.edu/");
//
//        /*
//         * Start the crawl. This is a blocking operation, meaning that your code
//         * will reach the line after this only when crawling is finished.
//         */
//        controller.start(Crawler.class, numberOfCrawlers);  
	}
}
