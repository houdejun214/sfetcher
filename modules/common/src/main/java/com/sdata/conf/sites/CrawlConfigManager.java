package com.sdata.conf.sites;

import java.util.Iterator;

import org.dom4j.Document;
import org.dom4j.DocumentException;
import org.dom4j.DocumentHelper;
import org.dom4j.Element;
import org.dom4j.io.SAXReader;

import com.lakeside.core.utils.FileUtils;
import com.lakeside.core.utils.StringUtils;
import com.sdata.core.Configuration;
import com.sdata.core.Constants;
import com.sdata.core.util.ApplicationResourceUtils;

/**
 * 
 * crawler参数配置类，包含配置参数的加载。
 * 
 * @author houdejun
 *
 */
public class CrawlConfigManager {
	
	private static final String crawlDefaultConfFile = "conf/crawl-default.xml";
	
	private Configuration defaultConf;
	
	private String crawlName;
	
	private CrawlConfig curCrawlSite;
	
	public Configuration getDefaultConf() {
		return defaultConf;
	}
	
	public String getDefaultConf(String key){
		if(this.defaultConf!=null){
			return this.defaultConf.get(key, null);
		}
		return null;
	}

	public void setDefaultConf(Configuration defaultConf) {
		this.defaultConf = defaultConf;
	}

	public CrawlConfig getCurCrawlSite(){
		if(curCrawlSite!=null){
			curCrawlSite.putConf("SiteName", crawlName);
			curCrawlSite.putConf("CrawlName", crawlName);
		}
		return curCrawlSite;
	}

	public void setCurCrawlSite(CrawlConfig curCrawlSite) {
		this.curCrawlSite = curCrawlSite;
	}
	
	public static CrawlConfigManager load(String crawlName,String siteName,boolean isTemplate) throws Exception{
		CrawlConfigManager config = new CrawlConfigManager();
		Configuration defaultConf = loadDefaultConfig();
		config.setDefaultConf(defaultConf);
		config.crawlName = crawlName;
		if(!StringUtils.isEmpty(crawlName)){
			try {
				Element siteEl = null;
				//template 
				if(isTemplate){
					siteEl = readSiteConfigElement("conf/crawl-template.xml");
					siteEl.addAttribute("name",crawlName);
				}else{
					siteEl = readSiteConfigElement("sites/crawl-"+crawlName+".xml");
				}
				if(siteEl == null){
					throw new RuntimeException("crawl site xml not found!");
				}
				
	        	CrawlConfig site = getCrawlSite(siteEl);
	        	site.setDefaultConf(defaultConf);
	        	// add site conf parameters
	        	getSetting(siteEl, site);
	        	
	        	// read parameter setting in template directory
	        	// this is usually used to override the crawler setting for current template 
	        	siteEl = readSiteConfigElement("template/"+crawlName+"/tpl-"+crawlName+"-conf.xml");
	        	if(siteEl!=null){
	        		getSetting(siteEl, site);
	        	}
	        	if(isTemplate){
		        	site.putConf("strategyPath","template/"+crawlName+"/tpl-"+crawlName+"-strategy.xml");
		        	site.putConf("datumPath","template/"+crawlName+"/tpl-"+crawlName+"-datum.xml");
		        	site.putConf("storePath","template/"+crawlName+"/tpl-"+crawlName+"-store.xml");
	        	}
				if(!StringUtils.isEmpty(siteName)){
					site.putConf("siteName", siteName);
					site.putConf(Constants.SOURCE, siteName);
				}
				site.putConf("crawlName", crawlName);
	    		config.setCurCrawlSite(site);
			} catch (Exception e) {
				throw e;
			}
		}
		return config;
	}
	
	public static CrawlConfigManager load(String crawlName) throws Exception {
		return load(crawlName,null,false);
	}

	public static Configuration loadFromPath(String path) {
		if(!StringUtils.isEmpty(path)){
			try {
				String file = ApplicationResourceUtils.getResourceUrl(CrawlConfigManager.class,path);
				SAXReader reader = new SAXReader();
				Document document = reader.read(file);
				return loadDocument(document);
			} catch (DocumentException e) {
				
			}
		}
		return new Configuration();
	}
	
	public static Configuration loadFromXml(String xml) {
		try {
			Document document = DocumentHelper.parseText(xml);
			return loadDocument(document);
		} catch (DocumentException e) {
			return new Configuration();
		}
	}
	
	public static Configuration loadDocument(Document document) {
		Configuration conf = new Configuration();
		Element root = document.getRootElement();
		Iterator<?> iterator = root.elementIterator();
		while(iterator.hasNext()){
			Element next = (Element)iterator.next();
			Property pro = getProperty(next);
			conf.put(pro.getName(), pro.getValue());
		}
		return conf;
	}

	/**
	 *  read configure document element from file
	 * 
	 * @param crawlSiteConfFile
	 * @return
	 * @throws DocumentException
	 */
	private static Element readSiteConfigElement(String crawlSiteConfFile)
			throws DocumentException {
		String file = ApplicationResourceUtils.getResourceUrl(CrawlConfigManager.class,crawlSiteConfFile);
		if(!FileUtils.exist(file)){
			return null;
		}
		// load xml config file 
		SAXReader reader = new SAXReader();
		Document document = reader.read(file);
		Element root = document.getRootElement();
		// get the site configuration
		Element siteEl = root.element("site");
		if(siteEl==null){
			return root;
		}
		return siteEl;
	}
	
	
	private static CrawlConfig getCrawlSite(Element el){
		String name = el.attributeValue("name");
		CrawlConfig site =new CrawlConfig(name);
		site.setHostUrl(el.attributeValue("hosturl"));
		site.setFetcherType(el.attributeValue("fetcher"));
		site.setParserType(el.attributeValue("parser"));
		site.setStorer(el.attributeValue("storer"));
		return site;
	}
	
	private static void getSetting(Element siteEl, CrawlConfig site) {
		Iterator<?> proIterator = siteEl.elementIterator();
		while(proIterator.hasNext()){
			Element proEl = (Element)proIterator.next();
			String name = proEl.getName();
			if("property".equals(name)){
				Property pro = getProperty(proEl);
				site.putConf(pro.getName(), pro.getValue());
			}else if("filter".equals(name)){
				String pos = proEl.attributeValue("pos");
				String filterclass = proEl.attributeValue("class");
				if(!StringUtils.isEmpty(pos) && !StringUtils.isEmpty(filterclass)){
					site.addFilter(pos,filterclass);
				}
			}
		}
	}
	
	public static Configuration loadDefaultConfig(){
		return loadFromPath(crawlDefaultConfFile);
	}
	
	private static Property getProperty(Element el){
		Property pro = new Property();
		Element name = el.element("name");
		if(name!=null){
			pro.setName(name.getTextTrim());
		}
		Element value = el.element("value");
		if(value!=null){
			pro.setValue(value.getTextTrim());
		}
		return pro;
	}
	
	private static class Property{
		private String name;
		private String value;
		public String getName() {
			return name;
		}
		public void setName(String name) {
			this.name = name;
		}
		public String getValue() {
			return value;
		}
		public void setValue(String value) {
			this.value = value;
		}
	}
}
