package com.sdata.core.parser.html.notify;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;

import com.sdata.context.config.Configuration;
import com.sdata.context.config.CrawlAppContext;
import com.sdata.core.parser.config.DatumConfig;
import com.sdata.core.parser.html.field.Field;
import com.sdata.util.BooleanEvaluator;


/**
 * @author zhufb
 *
 */
public class DatumNotify extends CrawlNotify{
	
	private String nullstr = "";
	private String contNullStr = "";
	private int DATUM_LIMIT_COUNT;
	private List<String> notNullFields =  new ArrayList<String>();
	private List<String> notContinuousNullFields = new ArrayList<String>();
	private Map<String,Integer> map = new HashMap<String,Integer>();
	private Configuration conf;
	
	public DatumNotify(Configuration conf,String notify){
		this.conf = conf;
		if(StringUtils.isEmpty(notify)){
			return;
		}
		this.DATUM_LIMIT_COUNT = CrawlAppContext.conf.getInt("crawler.datum.notify.limit", 100);
		String[] split = notify.split(",");
		for(String s:split){
			String[] split2 = s.split(":");
			if(split2.length !=2){
				continue;
			}
			if("null".equals(split2[0].toLowerCase())){
				this.setNull(split2[1]);
			}else if("contnull".equals(split2[0].toLowerCase())){
				this.setContNull(split2[1]);
			}
		}
	}
	
	@Override
	public void notify(Map<?,Object> data){
		boolean bNull = true;
		String strNull = nullstr;
		Iterator<String> notNullIter = notNullFields.iterator();
		while(notNullIter.hasNext()){
			String key = notNullIter.next();
			strNull = strNull.replaceAll(key, this.isNull(data, key));
		}
		if(!StringUtils.isEmpty(strNull)){
			bNull = BooleanEvaluator.eval(strNull);
		}
		
		if(!bNull){
			super.mail(nullNotifyContent());
			return;
		}
		
		Iterator<String> contNullIter = notContinuousNullFields.iterator();
		while(contNullIter.hasNext()){
			String key = contNullIter.next();
			String strb = this.isNull(data, key);
			if("1".equals(strb)){
				map.put(key, 0);
			}else{
				Integer times = map.get(key);
				if(times ==null){
					times = 0;
				}
				if(++times>DATUM_LIMIT_COUNT){
					super.mail(contNullNotifyContent(key));
					times = 0;
				}
				map.put(key, times);
			}
		}

	}
	
	private String nullNotifyContent(){
    	StringBuffer sb = new StringBuffer("Datum notify \n");
		sb.append("crawler ").append(CrawlAppContext.state.getCrawlName());
		sb.append("'s not null field  ").append(this.nullstr);
		sb.append(" has been null just now!");
		return sb.toString();
	}

	private String contNullNotifyContent(String field){
    	StringBuffer sb = new StringBuffer("Datum notify \n");
		sb.append("crawler ").append(CrawlAppContext.state.getCrawlName());
		sb.append("'s not continuation null field  ").append(field);
		sb.append(" has always been null so as to reach to limit ").append(this.DATUM_LIMIT_COUNT);
		return sb.toString();
	}
	
	private String isNull(Map data,String key){
		if("0".equals(isExists(data,key))){
			return "0";
		}
		if(data.get(key) == null||"".equals(data.get(key))){
			return "0";
		}
		return "1";
	}
	
	private String isExists(Map data,String key){
		if(!data.containsKey(key)){
			return "0";
		}
		return "1";
	}

	private void setNull(String nullStr) {
		this.nullstr = nullStr;
		String[] split = nullStr.split("[\\(\\)\\&\\|]");
		for(String s:split){
			if(!StringUtils.isEmpty(s)){
				notNullFields.add(s);
			}
		}
	}
	
	private void setContNull(String contNull) {
		this.contNullStr = contNull;
		// 当为*时，是所有字段都在考虑范围之内
		if("*".equals(contNull)){
			Iterator<Field> fields = DatumConfig.getInstance(conf).getFields();
			while(fields.hasNext()){
				Field next = fields.next();
				notContinuousNullFields.add(next.getName());
			}
		}else{
			String[] split = contNull.split("\\|");
			for(String s:split){
				if(!StringUtils.isEmpty(s)){
					notContinuousNullFields.add(s);
				}
			}
		}
	}

}