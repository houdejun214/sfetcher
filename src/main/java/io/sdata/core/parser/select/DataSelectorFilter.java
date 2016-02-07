package io.sdata.core.parser.select;

import java.util.regex.Matcher;

import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import com.lakeside.core.utils.StringUtils;

/**
 * this selector should be like ":[dt=服务]"
 * 
 * @author houdejun
 *
 */
public class DataSelectorFilter extends DataSelector {

	private String filters= null;
	private String internalSelector="";
	private String match = "";
	
	public DataSelectorFilter(String selector){
		this.selector = selector;
		Matcher matcher = SELECTOR_FILTER.matcher(selector);
		if(matcher.find()){
			filters = matcher.group(1);
			int index = filters.lastIndexOf("=");
			if(index<=0){
				throw new RuntimeException("filter selector syntax error!");
			}
			internalSelector = filters.substring(0,index);
			match = filters.substring(index+1).trim();
		}
	}
	
	@Override Object selectObject(Object inputObject){
		if(inputObject==null){
			return null;
		}
		Object value = null;
		if(inputObject instanceof Elements){
			Elements els = (Elements)inputObject;
			Elements result = new Elements();
			for(Element el: els){
				if(filter(el)){
					result.add(el);
				}
			}
			return result;
		}
		return value;
	}
	
	private boolean filter(Element el){
		Elements select = el.select(internalSelector);
		Element first = select.first();
		if(first==null){
			return false;
		}
		String text = first.text();
		text = StringUtils.trim(text," :：。.");
		if(match.endsWith(text)){
			return true;
		}
		return false;
	}
}
