package com.sdata.core.data.image;

import java.util.ArrayList;
import java.util.List;

import com.sdata.core.CrawlAppContext;

/**
 * @author zhufb
 * 
 * the producer of typical producer-consumer scenario 
 *
 */
public class ImageProducer{

	public void produce(String source,List<String> list){
		CrawlAppContext.db.insertImageQueue(list, source);
	}
	
	public void produce(String source,String item){
		List<String> list = new ArrayList<String>();
		list.add(item);
		this.produce(source,list);
	}
}
