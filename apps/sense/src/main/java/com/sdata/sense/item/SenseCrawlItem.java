package com.sdata.sense.item;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import net.sf.json.JSONObject;

import com.lakeside.core.utils.JSONUtils;
import com.lakeside.core.utils.MapUtils;
import com.lakeside.core.utils.StringUtils;
import com.lakeside.core.utils.UrlFormater;
import com.lakeside.core.utils.UrlUtils;
import com.lakeside.core.utils.time.DateFormat;
import com.lakeside.core.utils.time.DateTimeUtils;
import com.sdata.core.item.CrawlItem;
import com.sdata.core.item.CrawlItemEnum;

/**
 * 
 * Crawl item for crawler from SCMS
 * @author zhufb
 *
 */
public class SenseCrawlItem extends CrawlItem {
	
	public SenseCrawlItem(Map<String,Object> map){
		super(map);
		if(map == null){
			return;
		}
		this.id = MapUtils.getLong(map,"id");
		
		this.crawlerName = MapUtils.getString(map,"crawler_name");
		this.sourceName = MapUtils.getString(map,"source_name");
		
		this.entryUrl = MapUtils.getString(map,"entry_url");
		this.entryName = MapUtils.getString(map,"entry_name");
		
		this.params = MapUtils.getString(map,"parameters");
		this.fields = MapUtils.getString(map,"fields");
		
		this.priorityScore = MapUtils.getInt(map, "priority_score");
		this.objectId = MapUtils.getLong(map, "object_id");
		this.templeteId = MapUtils.getLong(map, "crawler_template_id");
		this.status = MapUtils.getString(map,"status");
		
		//Test
//		this.id = 1L;
//		this.crawlId = "statistic";
//		this.entryUrl = "http://app2.nea.gov.sg/anti-pollution-radiation-protection/air-pollution/psi/historical-psi-readings/year/%d/month/%d/day/%d";
//		this.entryName = "statistic";
//		this.parameterType = "1";
//		this.parameterValue = "dengue";
//		this.objectId = 28L;
		
		//Test end
		this.init();
	}
	
	private Long id;
	private String crawlerName;
	private String sourceName;
	private String entryUrl;
	private String entryName;
	private String  params;
	private String  fields;
	private Integer priorityScore;
	private Long objectId;	
	private Long templeteId;	
	private String  status;
	private Map<String,Object> paramMap = new HashMap<String, Object>();
	private Map<String,Object> fieldMap = new HashMap<String, Object>();
	private Date start;
	private Date end;
	
	public Long getId() {
		return id;
	}

	public String getCrawlerName() {
		return crawlerName;
	}

	public String getEntryUrl() {
		return entryUrl;
	}

	public Integer getPriorityScore() {
		return priorityScore;
	}

	public Long getObjectId() {
		return objectId;
	}

	public String getStatus() {
		return status;
	}
	
	public String getEntryName() {
		return entryName;
	}

	public Long getTempleteId() {
		return templeteId;
	}

	public String getSourceName() {
		return sourceName;
	}

	public boolean containParam(String p){
		return paramMap.containsKey(p);
	}
	
	public Map<String, Object> getParams() {
		return paramMap;
	}

	public Object getParam(String p) {
		return paramMap.get(p);
	}

	public void putParam(String key,Object val){
		paramMap.put(key, val);
	}
	
	public Map<String, Object> getFields() {
		return fieldMap;
	}

	public String getParamStr(){
		return paramMap.toString();
	}
	
	private void init() {
		if(JSONUtils.isValidJSON(params)){
			paramMap = JSONObject.fromObject(params);
		}
		if(JSONUtils.isValidJSON(fields)){
			fieldMap = JSONObject.fromObject(fields);
		}
		Object tr = paramMap.get(CrawlItemEnum.TIMERANGE.getName());
		if(tr==null){
			return;
		}
		String[] times = tr.toString().split("/");
		if(times.length!=2){
			return;
		}
		Object st = DateFormat.changeStrToDate(times[0]);
		if(st!=null&&st instanceof Date){
			start = (Date)st;
		}
		Object et = DateFormat.changeStrToDate(times[1]);
		if(et!=null&&et instanceof Date){
			end = (Date)et;
		}
	}

	public Date getStart() {
		return start;
	}

	public Date getEnd() {
		return end;
	}

	public String parse(String charset){
		if(StringUtils.isEmpty(charset)){
			charset = "UTF-8";
		}
		UrlFormater formater = new UrlFormater(this.entryUrl);
		Map<String,Object> params = new HashMap<String, Object>();
		for(Entry<String, Object> e:paramMap.entrySet()){
			params.put(e.getKey(), UrlUtils.encode(StringUtils.valueOf(e.getValue()),charset));
		}
		return formater.format(params);
	}
	

	public String parse(){
		return this.parse(null);
	}
	
	public String toString(){
		return SenseCrawlItem.class.getName();
	}
}
