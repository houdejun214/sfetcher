package com.sdata.core.parser.html.action;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import com.lakeside.core.utils.StringUtils;
import com.sdata.core.Constants;
import com.sdata.core.data.image.ImageClient;
import com.sdata.core.parser.html.context.IParserContext;

/**
 * @author zhufb
 *
 */
public class MediaAction {
	
	public Object saveImage(IParserContext context,Object obj){
		Object source = context.getVariable(Constants.SOURCE);
		if(source == null||"".equals(source.toString())){
			return obj;
		}
		List<String> images = getImages(obj);
		ImageClient.getInstance(context.getConfig()).save(source.toString(),images);
		return obj;
	}
	
	public void saveVideo(IParserContext context,Object obj){
		//TODO 
	}
	
	private List<String> getImages(Object obj){
		List<String> images = new ArrayList<String>();
		if(obj instanceof String) {
			if(!StringUtils.isEmpty(obj.toString())) {
				images.add(obj.toString());
			}
		}else if(obj instanceof Map){
			images.add(((Map)obj).get("url").toString());
		}else if(obj instanceof List){
			List<Map<String,Object>> result = new ArrayList<Map<String,Object>>();
			Iterator iterator = ((List) obj).iterator();
			while(iterator.hasNext()){
				Object next = iterator.next();
				if(next instanceof String ){
					images.add(String.valueOf(next));
				}else if(next instanceof Map ){
					images.add(((Map)next).get("url").toString());
				}
			}
		}
		return images;
	}
}
