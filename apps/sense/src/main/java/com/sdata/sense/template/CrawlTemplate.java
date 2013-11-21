package com.sdata.sense.template;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.lakeside.core.utils.MapUtils;
import com.sdata.core.CrawlAppContext;

/**
 * @author zhufb
 *
 */
public class CrawlTemplate {
	
	private static Object syn = new Object();
	private static Map<Long,CrawlTemplate> templates = null;
	public CrawlTemplate(Map<String,Object> map){
		if(map == null||map.size() == 0){
			return;
		}
		this.id = MapUtils.getLong(map,"id");
		this.config = MapUtils.getString(map,"xml_config");
		this.strategy = MapUtils.getString(map,"xml_strategy");
		this.datum = MapUtils.getString(map,"xml_datum");
		this.store = MapUtils.getString(map, "xml_store");
	}
	
	private Long id;
	private String config;
	private String strategy;
	private String datum;
	private String store;
	
	public Long getId() {
		return id;
	}
	public String getConfig() {
		return config;
	}
	public String getStrategy() {
		return strategy;
	}
	public String getDatum() {
		return datum;
	}
	public String getStore() {
		return store;
	}
	
	public static CrawlTemplate getTemplate(Long id){
		if(templates == null){
			synchronized (syn) {
				if(templates == null){
					CrawlTemplateDB db = new CrawlTemplateDB(CrawlAppContext.conf);
					templates = new HashMap<Long,CrawlTemplate>();
					List<Map<String, Object>> list = db.query();
					for(Map<String,Object> map:list){
						CrawlTemplate ct = new CrawlTemplate(map);
						templates.put(ct.getId(), ct);
					}
				}
			}
		}
		return templates.get(id);
	}
}
