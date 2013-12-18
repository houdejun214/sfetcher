package com.sdata.proxy;

import java.util.Date;
import java.util.Map;

import com.lakeside.core.utils.time.DateFormat;
import com.sdata.core.FetchDatum;
import com.sdata.core.data.trans.DataOptimizer;
import com.sdata.proxy.item.SenseCrawlItem;

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
//		if(item.getEnd()!=null&&pubTime.after(item.getEnd())){
//			return false;
//		}
		return true;
	}
	
	public void setMetadata(Map<String, Object> data){
		this.metadata = data;
	}
	
	public boolean prepare(){
		DataOptimizer dataOptimizer = new DataOptimizer(SenseConfig.getConfig(item));
		this.setMetadata(dataOptimizer.optimize(this.getMetadata()));
		if(super.getMetadata() == null){
			return false;
		}
		super.getMetadata().putAll(defaultData());
		return true;
	}
	
	public Map<String,Object> defaultData() {
		return this.item.toMap();
	}

	public String getIndexColumn() {
		return this.item.getIndexColumn();
	}
}
