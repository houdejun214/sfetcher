package com.sfetcher.core.parser.select;

import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;


/**
 * select the data value by jsoup selector syntax
 * 
 * @author houdejun
 *
 */
public class DataSelectorText extends DataSelector {
	
	
	@Override Object selectObject(Object inputObject, PageContext context){
		if(inputObject==null){
			return null;
		}
		Object value = null;
		if(inputObject instanceof Element){
			Element el = (Element)inputObject;
			value = el.text();
		}else if(inputObject instanceof Elements){
			Elements els = (Elements)inputObject;
			value = els.text();
		}else{
			value = inputObject.toString();
		}
		return value;
	}
}
