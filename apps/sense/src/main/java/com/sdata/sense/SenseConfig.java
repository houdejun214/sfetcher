package com.sdata.sense;

import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang.StringUtils;

import com.lakeside.core.utils.ApplicationResourceUtils;
import com.lakeside.core.utils.FileUtils;
import com.sdata.conf.sites.CrawlConfigManager;
import com.sdata.core.Configuration;
import com.sdata.core.Constants;
import com.sdata.core.CrawlAppContext;
import com.sdata.core.parser.html.config.DatumConfig;
import com.sdata.core.parser.html.config.StoreConfig;
import com.sdata.core.parser.html.config.StrategyConfig;
import com.sdata.sense.item.SenseCrawlItem;
import com.sdata.sense.template.CrawlTemplate;

/**
 * @author zhufb
 *
 */
public class SenseConfig {

	private static Map<String,Configuration> configMap = new HashMap<String,Configuration>();
	
	/**
	 * Configuration load
	 * 
	 */
	private static String CONF_PATH = "template/%s/tpl-%s-conf.xml";
	private static String STRATEGY_PATH = "template/%s/tpl-%s-strategy.xml";
	private static String DATUM_PATH = "template/%s/tpl-%s-datum.xml";
	private static String STORE_PATH = "template/%s/tpl-%s-store.xml";
	
	public static Configuration getConfig(String id){
		if(configMap.get(id) == null){
			synchronized (configMap) {
				if(configMap.get(id) == null){
					Configuration conf = new Configuration(CrawlAppContext.conf);
					conf.put(StrategyConfig.CONF_XML, getStrategyPath(id));
					conf.put(DatumConfig.CONF_XML, getDatumPath(id));
					conf.put(StoreConfig.CONF_XML, getStorePath(id));
					Configuration customConf = CrawlConfigManager.loadFromPath(getConfigPath(id));
					conf.putAll(customConf);
					configMap.put(id, conf);
				}
			}
		}
		return configMap.get(id);
	}
	
	public static Configuration getConfig(SenseCrawlItem item){
		String id = item.getCrawlerName();
		String entry = item.getEntryName();
		String key = getMergeKey(id,entry);
		if(configMap.get(key) == null){
			synchronized (configMap) {
				if(configMap.get(key) == null){
					Configuration conf = new Configuration(getConfig(id));
					conf.putAll(getConfiguration(item));
					conf.put(StrategyConfig.CONF_XML,getStrategy(item));
					conf.put(DatumConfig.CONF_XML, getDatum(item));
					conf.put(StoreConfig.CONF_XML, getStore(item));
					conf.put(Constants.SOURCE, entry);
					configMap.put(key, conf);
				}
			}
		}
		return configMap.get(key);
	}
	
	private static String getConfigPath(String id){
		return String.format(CONF_PATH,id,id);
	}
	
	private static String getConfigPath(String id,String entry){
		if(StringUtils.isEmpty(entry)){
			return getConfigPath(id);
		}
		String path = String.format(CONF_PATH, getMergePath(id,entry),entry);
		if(!check(path)){
			return getConfigPath(id);
		}
		return path;
	}

	private static Configuration getConfiguration(SenseCrawlItem item){
		CrawlTemplate template = CrawlTemplate.getTemplate(item.getTempleteId());
		if(template!=null&&!StringUtils.isEmpty(template.getConfig())){
			return CrawlConfigManager.loadFromXml(template.getConfig());
		}
		return CrawlConfigManager.loadFromPath(getConfigPath(item.getCrawlerName(),item.getEntryName()));
	}

	private static String getStrategyPath(String id){
		return String.format(STRATEGY_PATH,id,id);
	}
	
	private static String getStrategyPath(String id,String entry){
		String path = String.format(STRATEGY_PATH,getMergePath(id,entry),entry);
		if(!check(path)){
			path = getStrategyPath(id);
		}
		return path;
	}
	
	private static String getStrategy(SenseCrawlItem item){
		CrawlTemplate template = CrawlTemplate.getTemplate(item.getTempleteId());
		if(template!=null&&!StringUtils.isEmpty(template.getStrategy())){
			return template.getStrategy();
		}
		return getStrategyPath(item.getCrawlerName(), item.getEntryName());
	}

	private static String getDatumPath(String id){
		return String.format(DATUM_PATH,id,id);
	}
	
	private static String getDatumPath(String id,String entry){
		String path = String.format(DATUM_PATH,getMergePath(id,entry),entry);
		if(!check(path)){
			path = getDatumPath(id);
		}
		return path;
	}

	private static String getDatum(SenseCrawlItem item){
		CrawlTemplate template = CrawlTemplate.getTemplate(item.getTempleteId());
		if(template!=null&&!StringUtils.isEmpty(template.getDatum())){
			return template.getDatum();
		}
		return getDatumPath(item.getCrawlerName(), item.getEntryName());
	}

	private static String getStorePath(String id){
		return String.format(STORE_PATH,id,id);
	}
	
	private static String getStorePath(String id,String entry){
		String path = String.format(STORE_PATH,getMergePath(id,entry),entry);
		if(!check(path)){
			path = getStorePath(id);
		}
		return path;
	}
	
	private static String getStore(SenseCrawlItem item){
		CrawlTemplate template = CrawlTemplate.getTemplate(item.getTempleteId());
		if(template!=null&&!StringUtils.isEmpty(template.getStore())){
			return template.getStore();
		}
		return getStorePath(item.getCrawlerName(), item.getEntryName());
	}
	
	private static String getMergePath(String id,String entry){
		String result = id;
		if(!StringUtils.isEmpty(entry)){
			result =  id.concat("/").concat(entry);
		}
		return result;
	}

	
	private static String getMergeKey(String id,String entry){
		String result = id;
		if(!StringUtils.isEmpty(entry)){
			result =  id.concat(":").concat(entry);
		}
		return result;
	}
	
	private static boolean check(String path){
		return FileUtils.exist(ApplicationResourceUtils.getResourceUrl(path));
	}
}
