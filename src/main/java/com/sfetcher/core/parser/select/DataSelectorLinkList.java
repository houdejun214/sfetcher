package com.sfetcher.core.parser.select;

import java.util.ArrayList;
import java.util.List;

import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

public class DataSelectorLinkList extends DataSelectorLink {

	@Override
	Object selectObject(Object inputObject, PageContext context) {
		if(inputObject==null){
			return null;
		}
		List<String> links = new ArrayList<String>();
		if(inputObject instanceof Element){
			Element el = (Element)inputObject;
			String link = getLink(el);
			links.add(link);
		}else if(inputObject instanceof Elements){
			Elements els = (Elements)inputObject;
			for(Element el:els){
				String link = getLink(el);
				links.add(link);
			}
		}
		return links;
	}

}
