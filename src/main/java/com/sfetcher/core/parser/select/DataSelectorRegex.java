package com.sfetcher.core.parser.select;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.jsoup.nodes.Element;


public class DataSelectorRegex extends DataSelector {
	
	private Pattern regx=null;
	
	public DataSelectorRegex(String selector){
		this.selector = selector;
		Matcher matcher = SELECTOR_REGX.matcher(selector);
		if(matcher.find()){
			String regxStr = matcher.group(1);
			regx = Pattern.compile(regxStr);
		}
	}
	
	@Override Object selectObject(Object inputObject, PageContext context){
		if(inputObject==null){
			return null;
		}
		String inputTxt = "";
		if(inputObject instanceof Element){
			inputTxt = ((Element)inputObject).html();
		}else{
			inputObject = this.getFirstObjectIfList(inputObject);
			inputTxt = inputObject.toString();
		}
		Matcher matcher = regx.matcher(inputTxt);
		List<String> results = new ArrayList<String>();
		if(matcher.find()){
			results.add(matcher.group());
			for(int i=0;i<matcher.groupCount();i++){
				results.add(matcher.group(i+1));
			}
			return results;
		}
		return null;
	}
}
