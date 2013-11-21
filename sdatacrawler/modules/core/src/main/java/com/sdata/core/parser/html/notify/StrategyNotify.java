package com.sdata.core.parser.html.notify;

import java.util.Map;

import org.apache.commons.lang.StringUtils;

import com.sdata.core.CrawlAppContext;
import com.sdata.core.parser.html.field.Tags;


/**
 * @author zhufb
 *
 */
public class StrategyNotify extends CrawlNotify{

	private int count = 0;
	private String notifyTag;
	private int notifyLimit = 100;
	public StrategyNotify(String notify){
		if(StringUtils.isEmpty(notify)){
			return;
		}
		String[] notifys = notify.split(":");
		this.notifyTag = notifys[0];
		if(notifys.length == 2){
			this.notifyLimit = Integer.valueOf(notifys[1]);
		}
	}
	
	@Override
	public void notify(Map<?, Object> data) {
		Tags tag = Tags.getEnum(notifyTag);
		if(tag == null){
			return;
		}
		if(data.containsKey(tag)&&data.get(tag)!=null){
			count = 0;
			return;
		}
		if(++count>notifyLimit){
			super.mail(this.notifyContent());
			this.count = 0;
		}
	}
	
	private String notifyContent(){
		StringBuffer sb = new StringBuffer("Strategy notify \n");
		sb.append("crawler ").append(CrawlAppContext.state.getCrawlName());
		sb.append("'s strategy tag ").append(this.notifyTag);
		sb.append(" has always been null so as to reach to the limit count ").append(notifyLimit);
		return sb.toString();
	}
	
	
}