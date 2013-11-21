package com.sdata.sense;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import com.lakeside.core.utils.time.DateFormat;
import com.sdata.core.FetchDatum;
import com.sdata.core.data.FieldProcess;
import com.sdata.sense.item.SenseCrawlItem;

/**
 * @author zhufb
 *
 */
public class SenseFetchDatum extends FetchDatum {
	
	private SenseCrawlItem item;
	
	public SenseCrawlItem getCrawlItem() {
		return item;
	}

	public void setCrawlItem(SenseCrawlItem crawlItem) {
		this.item = crawlItem;
	}
	
	public boolean valid(){
		Object opub = DateFormat.changeStrToDate(super.getMeta(Constants.PUB_TIME));
		if(opub == null||!(opub instanceof Date)){
			return false;
		}
		Date pubTime =(Date)opub;
		if(item.getStart()!=null&&pubTime.before(item.getStart())){
			return false;
		}
		if(item.getEnd()!=null&&pubTime.after(item.getEnd())){
			return false;
		}
		return true;
	}
	
	public void setMetadata(Map<String, Object> data){
		this.metadata = data;
	}
	
	public boolean prepare(){
		super.getMetadata().putAll(defaultData());
		FieldProcess fieldProcess = new FieldProcess(SenseConfig.getConfig(item));
		this.setMetadata(fieldProcess.fieldReduce(this.getMetadata()));
		if(super.getMetadata() == null){
			return false;
		}
		return true;
	}
	
	public Map<String,Object> defaultData() {
		Map<String,Object> result = new HashMap<String, Object>();
		result.put(Constants.DATA_TAGS_FROM_SOURCE,this.item.getSourceName());
		result.put(Constants.DATA_TAGS_FROM_OBJECT_ID,this.item.getObjectId());
		result.put(Constants.DATA_TAGS_FROM_PARAM_TYPE,this.item.getObjectId());
		result.put(Constants.DATA_INDEX_COLUMN,getIndexColumn());
		result.put(Constants.FETCH_TIME,new Date());
		result.putAll(item.getFields());
		result.putAll(item.getParams());
		return result;
	}

	public String getIndexColumn() {
		return String.valueOf(item.getParamStr().hashCode());
	}
}
