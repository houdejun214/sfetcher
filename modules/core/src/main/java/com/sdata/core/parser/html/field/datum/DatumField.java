package com.sdata.core.parser.html.field.datum;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import com.sdata.core.parser.html.util.Documents;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.lakeside.core.utils.MapUtils;
import com.lakeside.core.utils.StringUtils;
import com.sdata.context.parser.IParserContext;
import com.sdata.core.parser.html.context.DatumContext;
import com.sdata.core.parser.html.field.Field;
import com.sdata.core.parser.html.field.FieldDefault;
import com.sdata.core.parser.select.DataSelector;
import com.sdata.core.parser.select.DataSelectorPipleBuilder;

/**
 * @author zhufb
 *
 */
public class DatumField extends FieldDefault implements Field{
	private static final Logger log = LoggerFactory.getLogger("SdataCrawler.DatumField");
	protected String name;
	protected String type;
	protected String from;
	
	protected String next;
	protected DataSelector nextSelector;
	private Map<Object,String> pageMap = new HashMap<Object,String>();
	
	protected String[] fromFields;
	protected String index;
	protected String indexOnly;
	protected DataSelector dataSelector;
	protected List<Field> subFields = new ArrayList<Field>(); 

	public Object getData(IParserContext context,Element doc){
		Object result = this.getSelectValue(context,doc);
		
		// if field has next attribute, then string append
		if(this.hasNext(context,doc)&&result!=null){
			StringBuffer sb = new StringBuffer();
			sb.append(result);
			while(true){
				doc = this.getNextDoc(context,doc);
				sb.append(this.getSelectValue(context, doc));
				if(!this.hasNext(context,doc)){
					break;
				}
			}
			result = sb.toString();
		}
		
		// has child it's map
		if(result!=null&&result instanceof Elements&&this.hasChild()){
			Map<String,Object> map = new HashMap<String,Object>();
			List<Field> childs = this.getChilds();
			Iterator<Field> iterator = childs.iterator();
			while(iterator.hasNext()){
				DatumField f = (DatumField)iterator.next();
				Object td = f.getData(context, Documents.parseDocument(((Elements) result).html()));
				map.put(f.getName(), td);
			}
			result = map;
		}
		return super.refine(context,result);
	}
	
	public Object transData(IParserContext context,Map<String,Object> data){
		//
		Object result = getInterData(context,data);
		
		// has child it's map
		if(result!=null&&this.hasChild()&&result instanceof Map){
			Map<String,Object> map = new HashMap<String,Object>();
			List<Field> childs = this.getChilds();
			Iterator<Field> iterator = childs.iterator();
			while(iterator.hasNext()){
				DatumField f = (DatumField)iterator.next();
				Object td = f.transData(context,(Map)result);
				if(td!=null&&!"".equals(td)){
					map.put(f.getName(), td);
				}
			}
			result = map.size() == 0?null:map;
		}
		return super.refine(context,result);
	}
	
	public void addChildField(Field field){
		this.subFields.add(field);
	}
	
	public void setChilds(List<Field> subFields) {
		this.subFields = subFields;
	}

	public List<Field> getChilds(){
		return this.subFields;
	}
	
	public boolean hasChild(){
		return this.subFields.size()>0?true:false;
	}
	
	public int getChildsSize(){
		return this.subFields.size();
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getType() {
		return type;
	}

	public void setType(String type) {
		this.type = type;
	}

	public String getFrom() {
		return from;
	}

	public void setFrom(String from) {
		this.from = from;
		if(!StringUtils.isEmpty(from)){
			this.fromFields = from.split(",");
		}
	}

	public String getIndex() {
		return index;
	}

	public void setIndex(String index) {
		this.index = index;
	}

	public String getIndexOnly() {
		return indexOnly;
	}

	public void setIndexOnly(String indexOnly) {
		this.indexOnly = indexOnly;
	}

	public String getNext() {
		return next;
	}

	protected Object getNext(IParserContext context,Element doc){
		if(!StringUtils.isEmpty(next)){
			nextSelector.setContext(context);
			return nextSelector.select(doc);
		}
		return null;
	}

	protected boolean hasNext(IParserContext context,Element doc){
		if(StringUtils.isEmpty(this.getNext())){
			return false;
		}
		Object next = this.getNext(context, doc);
		if(next == null){
			return false;
		}
		if(pageMap.containsKey(next)){
			return false;
		}
		pageMap.put(next, "");
		return true;
	}
	
	public void setNext(String next) {
		this.next = next;
		this.nextSelector = DataSelectorPipleBuilder.build(next);
	}

	protected Document getNextDoc(IParserContext context,Element doc){
		Object nextUrl = this.getNext(context, doc);
		return Documents.getDocument(nextUrl == null ? "" : nextUrl.toString(), context.getHttpHeader());
	}

	protected Object getInterData(IParserContext context,Map<String,Object> data){
		Object result = null;
		if((result == null||"".equals(result.toString().trim()))&&!StringUtils.isEmpty(from)){
			// 只有一个字段
			if(fromFields.length == 1){
				result =  MapUtils.getInter(data,from);
			}else{
				//多个字段时，是字符串
				StringBuffer sb = new StringBuffer();
				for(String f:fromFields){
					Object v = MapUtils.getInter(data, f);
					if(v != null&&!StringUtils.isEmpty(v.toString())){
						sb.append(v).append(",");
					}
				}
				if(sb.length()>0){
					result = sb.substring(0, sb.length()-1);
				}
			}
		}
		if((result == null||"".equals(result.toString().trim()))&&!StringUtils.isEmpty(name)){
			result = MapUtils.getInter(data, name);
		}
		if((result == null||"".equals(result.toString().trim()))&&!StringUtils.isEmpty(value)){
			result =  value;
		}
		if((result == null||"".equals(result.toString().trim()))&&!StringUtils.isEmpty(ref)){
			result =  ((DatumContext)context).getTransValue(ref,data);
		}
		return result;
	}
	
}