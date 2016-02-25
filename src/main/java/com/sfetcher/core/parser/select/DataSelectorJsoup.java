package com.sfetcher.core.parser.select;

import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import com.lakeside.core.utils.StringUtils;


/**
 * select the data value by jsoup selector syntax
 * 
 * @author houdejun
 *
 */
public class DataSelectorJsoup extends DataSelector {
	
	private boolean nextSbling = false;
	private boolean preSbling = false;
	
	public DataSelectorJsoup(String txt) {
		this.selector = StringUtils.trim(txt);
		if(this.selector.startsWith("+")){
			nextSbling = true;
			this.selector = StringUtils.trim(StringUtils.chompHeader(selector, "+"));
		}else if(this.selector.startsWith("-")){
			preSbling = true;
			this.selector = StringUtils.trim(StringUtils.chompHeader(selector, "-"));
		}
	}

	@Override Object selectObject(Object inputObject, PageContext context){
		if(inputObject==null){
			return null;
		}
		Elements select = null;
		if(inputObject instanceof Element){
			select = selectElement(inputObject);
		}else if(inputObject instanceof Elements){
			Elements els = (Elements)inputObject;
			if(els.size() == 1){
				select = selectElement(els.first());
			}else{
				select = els.select(this.selector);
			}
		}
		return select;
	}

	private Elements selectElement(Object inputObject) {
		if(inputObject == null){
			return null;
		}
		Element el = (Element)inputObject;
		if(el instanceof Document){
			return el.select(this.selector);
		}
		if(nextSbling){
			el = el.nextElementSibling();
		}else if(preSbling){
			el = el.previousElementSibling();
		}
		if(el!=null){
			return el.select(this.selector);
		}else{
			return null;
		}
	}
}
