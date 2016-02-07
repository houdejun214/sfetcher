package io.sdata.core.parser.select;

import java.util.regex.Matcher;

import org.jsoup.nodes.Element;

public class DataSelectorAttribute extends DataSelector {

	private String attri= null;
	
	public DataSelectorAttribute(String selector){
		this.selector = selector;
		Matcher matcher = SELECTOR_ATTRI.matcher(selector);
		if(matcher.find()){
			String group = matcher.group(1);
			attri = group;
		}
	}
	
	@Override Object selectObject(Object inputObject){
		if(inputObject==null){
			return null;
		}
		inputObject = this.getFirstObjectIfList(inputObject);
		Object value = null;
		if(inputObject instanceof Element){
			Element el = (Element)inputObject;
			value = el.attr(attri);
		}
//		else{
//			throw new RuntimeException("the input object must be a Jsoup Object.");
//		}
		return value;
	}
}
