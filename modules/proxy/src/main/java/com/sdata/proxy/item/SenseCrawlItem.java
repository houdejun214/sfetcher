package com.sdata.proxy.item;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import net.sf.json.JSONObject;

import com.lakeside.core.utils.JSONUtils;
import com.lakeside.core.utils.MapUtils;
import com.lakeside.core.utils.time.DateFormat;
import com.sdata.core.item.CrawlItem;
import com.sdata.core.item.CrawlItemEnum;
import com.sdata.proxy.Constants;

/**
 * 
 * Crawl item for crawler from SCMS
 * @author zhufb
 *
 */
public abstract class SenseCrawlItem extends CrawlItem {
	
	public SenseCrawlItem(Map<String,Object> map){
		super(map);
		if(map == null){
			return;
		}
		this.crawlerName = MapUtils.getString(map,"crawler_name");
		this.sourceName = MapUtils.getString(map,"source_name");
		this.entryName = MapUtils.getString(map,"entry_name");
		this.entryUrl = MapUtils.getString(map,"entry_url");
		this.params = MapUtils.getString(map,"parameters");
		this.priorityScore = MapUtils.getInt(map, "priority_score");
		this.templeteId = MapUtils.getLong(map, "crawler_template_id");
		//Test end
		this.initParent();
	}
	
	protected String crawlerName;
	protected String sourceName;
	protected String entryName;
	protected String entryUrl;
	protected String  params;
	protected Integer priorityScore;
	protected Long templeteId;	
	protected Map<String,Object> paramMap = new HashMap<String, Object>();
	
	public String getCrawlerName() {
		return crawlerName;
	}

	public Integer getPriorityScore() {
		return priorityScore;
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
	
	public String getParamStr(){
		return paramMap.toString();
	}
	
	public String getEntryName() {
		return entryName;
	}
	
	public String getEntryUrl() {
		return entryUrl;
	}

	private void initParent() {
		if(JSONUtils.isValidJSON(params)){
			paramMap = JSONObject.fromObject(params);
		}
		Object tr = paramMap.get(CrawlItemEnum.TIMERANGE.getName());
		if(tr==null){
			return;
		}
		String[] times = tr.toString().split("/");
		if(times.length!=2){
			return;
		}
		Object st = DateFormat.strToDate(times[0]);
		if(st!=null&&st instanceof Date){
			start = (Date)st;
		}
		Object et = DateFormat.strToDate(times[1]);
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

	public String toString(){
		StringBuffer sb = new StringBuffer();
		sb.append(" id:").append(this.getId());
		sb.append(" crawler:").append(this.getCrawlerName());
		sb.append(" source:").append(this.getSourceName());
		sb.append(" params:").append(this.getParamStr());
		return sb.toString();
	}
	
	public Map<String,Object> toMap() {
		Map<String,Object> result = new HashMap<String, Object>();
		result.put(Constants.DATA_TAGS_FROM_SOURCE,this.getSourceName());
		result.put(Constants.DATA_INDEX_COLUMN,getIndexColumn());
		result.put(Constants.FETCH_TIME,new Date());
		result.putAll(this.getParams());
		return result;
	}

	public abstract String parse();
	
	
	public String getIndexColumn() {
		return String.valueOf(this.getParamStr().hashCode());
	}
}
