package com.sdata.core.parser.select;

import com.sdata.core.parser.html.context.IParserContext;


public class DataSelectorPipleBuilder {
	
	
	public static DataSelector build(String selector){
		
		return build(selector,null);
	}
	
	public static DataSelector build(String selector,IParserContext context){
		
		DataSelector headSelector= null;
		DataSelector preSelector = null;
		String[] splits = selector.split("\\|");
		for(String syntax:splits){
			String txt = syntax.trim();
			DataSelector curSelector=null;
			if(DataSelector.match(DataSelector.SELECTOR_ARRAYS,txt)){
				curSelector = new DataSelectorArrays(txt);
			}else if(DataSelector.match(DataSelector.SELECTOR_ATTRI,txt)){
				curSelector = new DataSelectorAttribute(txt);
			}else if(DataSelector.match(DataSelector.SELECTOR_TEXT,txt)){
				curSelector = new DataSelectorText();
			}else if(DataSelector.match(DataSelector.SELECTOR_HTML,txt)){
				curSelector = new DataSelectorHtml();
			}else if(DataSelector.match(DataSelector.SELECTOR_LINK,txt)){
				curSelector = new DataSelectorLink();
			}else if(DataSelector.match(DataSelector.SELECTOR_LINKLIST,txt)){
				curSelector = new DataSelectorLinkList();
			}else if(DataSelector.match(DataSelector.SELECTOR_REGX,txt)){
				curSelector = new DataSelectorRegex(txt);
			}else if(DataSelector.match(DataSelector.SELECTOR_FILTER,txt)){
				curSelector = new DataSelectorFilter(txt);
			}else if(DataSelector.match(DataSelector.SELECTOR_FORMAT,txt)){
				curSelector = new DataSelectorFormat(txt);
			}else{
				curSelector = new DataSelectorJsoup(txt);
			}
			if(headSelector == null){
				curSelector.setContext(context);
				headSelector = curSelector;
			}else{
				preSelector.setNext(curSelector);
			}
			preSelector = curSelector;
		}
		return headSelector;
	}
}
