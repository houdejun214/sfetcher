package com.sdata.core.parser.select;

import org.jsoup.nodes.Element;


/**
 * select the link string from a jsoup select result
 * 
 * @author houdejun
 *
 */
public class DataSelectorLink extends DataSelector {
	
	@Override Object selectObject(Object inputObject){
		if(inputObject==null){
			return null;
		}
		inputObject = this.getFirstObjectIfList(inputObject);
		if(inputObject instanceof Element){
			Element el = (Element)inputObject;
			String link = getLink(el);
			return link;
		}else{
		}
		return inputObject;
	}

	protected String getLink(Element el) {
		String link="";
		if("img".equals(el.tagName())){
			link = el.absUrl("src");
			
		}else if("a".equals(el.tagName()) || "link".equals(el.tagName())){
			if("".equals(el.baseUri())){
				link = el.attr("href");
			}else{
				link = el.absUrl("href");
			}
		}
		return link;
	}
}
