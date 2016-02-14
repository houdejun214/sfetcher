package io.sdata.core.parser.select;

public class DataSelectorPipleBuilder {
	
	public static DataSelector build(String selector){
		
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
				headSelector = curSelector;
			}else{
				preSelector.setNext(curSelector);
			}
			preSelector = curSelector;
		}
		return headSelector;
	}
}
