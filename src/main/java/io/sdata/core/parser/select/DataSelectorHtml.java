package io.sdata.core.parser.select;

import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;


/**
 * select the html value by jsoup selector syntax
 * 
 * @author houdejun
 *
 */
public class DataSelectorHtml extends DataSelector {
	
	
	@Override Object selectObject(Object inputObject, PageContext context){
		if(inputObject==null){
			return null;
		}
		Object value = null;
		if(inputObject instanceof Element){
			Element el = (Element)inputObject;
			value = el.html();
		}else if(inputObject instanceof Elements){
			Elements els = (Elements)inputObject;
			value = els.html();
		}else{
			value = inputObject.toString();
		}
		return value;
	}
}
