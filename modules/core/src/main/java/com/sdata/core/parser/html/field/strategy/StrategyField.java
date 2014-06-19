package com.sdata.core.parser.html.field.strategy;

import java.net.MalformedURLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import com.lakeside.core.utils.UrlUtils;
import net.sf.json.JSONObject;

import org.apache.commons.lang.StringUtils;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import com.lakeside.core.utils.MapUtils;
import com.sdata.context.parser.IParserContext;
import com.sdata.core.parser.html.field.Field;
import com.sdata.core.parser.html.field.FieldDefault;

/**
 * @author zhufb
 *
 */
public abstract class StrategyField  extends FieldDefault implements Field {
	
	protected String path;
	protected List<Field> subFields = new ArrayList<Field>(); 
	protected IParserContext context;
    protected String match;

    public String getMatch() {
        return match;
    }

    public void setMatch(String match) {
        this.match = match;
    }

    /**
	 * 根据context和Json爬取数据
	 * 
	 * @param context
	 * @param json
	 * @return
	 */
	public Object getData(IParserContext context,JSONObject json){
		if(this.hasChild()){
			return getChildsData(json);
		}
		if(StringUtils.isEmpty(path)){
			return null;
		}
		return MapUtils.getInter(json, path);
	}
	
	public Object getData(IParserContext context, Element doc) {
		this.context = context;
        if (this.match(doc)) {
            Object result = super.getSelectValue(context, doc);
            // has child it's map
            if(result!=null&&result instanceof Elements&&this.hasChild()){
                List<Object> resultList = new ArrayList<Object>();
                Iterator<Element> iterator = ((Elements) result).iterator();
                while(iterator.hasNext()){
                    Element next = (Element)iterator.next();
                    Map<String, Object> childsData = this.getChildsData(next);
                    resultList.add(childsData);
                }
                result = resultList;
            }
            return super.refine(context,result);
        }
        return null;
	}

    protected boolean match(Element doc) {
        if (StringUtils.isNotEmpty(match)) {
            String url = doc.baseUri();
            String domainName = null;
            try {
                domainName = UrlUtils.getDomainName(url);
            } catch (MalformedURLException e) {
                e.printStackTrace();
                return false;
            }
            if(match.matches(domainName)) {
                return true;
            }
            return false;
        }
        return true;
    }

    private Map<String,Object> getChildsData(Element element){
		Map<String,Object> map = new HashMap<String,Object>();
		List<Field> childs = this.getChilds();
		Iterator<Field> iterator = childs.iterator();
		while(iterator.hasNext()){
			Field f = (Field)iterator.next();
			Object td = f.getData(context,element);
			map.put(f.getName(), td);
		}
		return map;
	}

	private Map<String,Object> getChildsData(JSONObject json){
		Map<String,Object> map = new HashMap<String,Object>();
		List<Field> childs = this.getChilds();
		Iterator<Field> iterator = childs.iterator();
		while(iterator.hasNext()){
			StrategyField f = (StrategyField)iterator.next();
			Object td = f.getData(context,json);
			map.put(f.getName(), td);
		}
		return map;
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

	public String getPath() {
		return path;
	}

	public void setPath(String path) {
		this.path = path;
	}
}
