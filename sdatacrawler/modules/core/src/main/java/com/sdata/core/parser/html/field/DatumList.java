package com.sdata.core.parser.html.field;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;

import com.sdata.core.parser.html.context.IParserContext;
import com.sdata.core.parser.html.util.DocumentUtils;
import com.sdata.core.parser.select.DataSelector;
import com.sdata.core.parser.select.DataSelectorPipleBuilder;

/**
 * @author zhufb
 *
 */
public class DatumList extends DatumField{

//	private static final Logger log = LoggerFactory.getLogger("SdataCrawler.DatumList");
	protected String link;
	protected DataSelector linkSelector;
	public Object getData(IParserContext context,Element doc){
		List<Object> result = new ArrayList<Object>();
		//check link attribute
		if(this.hasLink(context,doc)){
			doc = this.getLinkDoc(context,doc);
		}
		while(doc!= null){
			// parse list attribute
			this.putListData(context,result, doc);
			// check has next
			if(!this.hasNext(context,doc)){
				doc = null;
			}else{
				doc = this.getNextDoc(context,doc);
			}
		}
		return refine(context,result);
	}
	
	public Object transData(IParserContext context,Map<String,Object> data){
		List<Object> list = new ArrayList<Object>();
		Object result = this.getInterData(context,data);
		
		if(result != null&&this.hasChild()&&result instanceof List){
			Iterator dataIter = ((List)result).iterator();
			while(dataIter.hasNext()){
				Object oneData = dataIter.next();
				if(!(oneData instanceof Map)){
					continue;
				}
				Iterator<Field> iterator = getChilds().iterator();
				while(iterator.hasNext()){
					DatumItem itm = (DatumItem)iterator.next();
					Object v = itm.transData(context,(Map)oneData);
					if(v == null){
						continue;
					}else if(v instanceof List){
						list.addAll((List)v);
					}else{
						list.add(v);
					}
				}
			}
		}
		return refine(context,list.size() == 0?null:list);
	}
	
	private boolean hasLink(IParserContext context,Element doc){
		if(StringUtils.isEmpty(this.getLink())){
			return false;
		}
		Object link = this.getLink(context, doc);
		if(link == null){
			return false;
		}
		return true;
	}
	
	private Document getLinkDoc(IParserContext context,Element doc){
		Object linkUrl = this.getLink(context, doc);
		if(context.hasVariable("Cookie")){
			
		}
		return DocumentUtils.getDocument(linkUrl==null?"":linkUrl.toString(),context.getHttpHeader());
	}
	
	private void putListData(IParserContext context,List<Object> result,Element doc){
		Object data = super.getSelectValue(context, doc);
		if( data == null){
			putChildsData(context,result, doc);
		}else if(data instanceof Element){
			putChildsData(context,result,(Element)data);
		}else if(data instanceof List){
			Iterator iterator = ((List) data).iterator();
			while(iterator.hasNext()){
				Element next = (Element)iterator.next();
				this.putChildsData(context,result, next);
			}
		}
	}
	
	private void putChildsData(IParserContext context,List<Object> result,Element doc){
		List<Field> childs = this.getChilds();
		Iterator<Field> iterator = childs.iterator();
		while(iterator.hasNext()){
			Field f = iterator.next();
			Object data = f.getData(context, doc);
			if(data instanceof List){
				result.addAll((List)data);
			}else{
				result.add(data);
			}
		}
	}
	
	public Object getLink(IParserContext context,Element doc){
		if(!StringUtils.isEmpty(link)){
			linkSelector.setContext(context);
			return linkSelector.select(doc);
		}
		return null;
	}

	public String getLink() {
		return link;
	}
	
	public void setLink(String link) {
		this.link = link;
		this.linkSelector = DataSelectorPipleBuilder.build(link);
	}
	
}