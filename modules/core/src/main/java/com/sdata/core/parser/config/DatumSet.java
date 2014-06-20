package com.sdata.core.parser.config;

import com.google.common.collect.Lists;
import com.lakeside.core.utils.UrlUtils;
import com.sdata.core.parser.html.field.Field;
import com.sdata.core.parser.html.field.datum.DatumField;
import com.sdata.core.parser.html.util.XmlFieldUtils;
import com.sdata.core.parser.select.DataSelectorFormat;
import org.apache.commons.lang.StringUtils;
import org.dom4j.Element;

import java.net.MalformedURLException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by dejun on 18/06/14.
 */
public class DatumSet {
    private List<Field> list = Lists.newArrayList();
    private Map<String,Field> fieldMap = new ConcurrentHashMap();
    private DatumFilter datumFilter;
    private String match;
    public DatumSet(Element root){
        if(root.getQName().getName().equals("datum")) {
            match = root.attributeValue("match");
            //add filter
            this.addDatumFilter(root);
            Iterator<Element> iterator = root.elementIterator();
            while (iterator.hasNext()) {
                Element next = (Element) iterator.next();
                list.add(XmlFieldUtils.getFieldFromXml(next));
            }
        }else {
            throw new RuntimeException("not a datum element");
        }
    }

    private void addDatumFilter(Element root){
        String v = root.attributeValue("filter");
        datumFilter = new DatumFilter(v);
    }

    public Field getField(String name) {
        if(StringUtils.isEmpty(name)){
            return null;
        }
        Iterator<Field> fields = getFields();
        while(fields.hasNext()){
            Field field = fields.next();
            Object str = field.getName();
            if(str!=null&&str.toString().equals(name)){
                return field;
            }
        }
        return null;
    }

    public Iterator<Field> getFields() {
        return list.iterator();
    }

    public Map<String,Field> getFieldMap() {
        if(fieldMap.size() == 0){
            Iterator<Field> fields = getFields();
            while(fields.hasNext()){
                this.putMap((DatumField)fields.next());
            }
        }
        return fieldMap;
    }

    private void putMap(DatumField field){
        Object str = field.getName();
        if(str!=null&&!StringUtils.isEmpty(str.toString())){
            fieldMap.put(str.toString(), field);
        }
    }

    public DatumFilter getDatumFilter() {
        return datumFilter;
    }

    public boolean match(org.jsoup.nodes.Element doc) {
        if (StringUtils.isNotEmpty(match)) {
            String url = doc.baseUri();
            return this.match(url);
        }
        return true;
    }

    public boolean match(String url) {
        if (StringUtils.isNotEmpty(match)) {
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

    public boolean needFetchContent(){
        Iterator<Field> fields = getFields();
        while(fields.hasNext()){
            Field field = fields.next();
          if(!(field.getDataSelector() instanceof DataSelectorFormat)){
            return true;
          }
        }
        return false;
    }
}
