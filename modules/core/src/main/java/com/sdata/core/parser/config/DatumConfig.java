package com.sdata.core.parser.config;

import com.google.common.collect.Lists;
import com.sdata.context.config.Configuration;
import com.sdata.context.parser.config.AbstractConfig;
import com.sdata.core.parser.html.field.Field;
import org.dom4j.Document;
import org.dom4j.Element;

import java.util.*;

/**
 * @author zhufb
 */
public class DatumConfig extends AbstractConfig {
    private List<Field> empty = Lists.newArrayList();

    private List<DatumSet> datums;
    public final static String CONF_XML = "datumXml";
    private final static Map<Configuration, DatumConfig> configMap = new HashMap<Configuration, DatumConfig>();

    public static DatumConfig getInstance(Configuration conf) {
        if (!configMap.containsKey(conf)) {
            synchronized (configMap) {
                if (!configMap.containsKey(conf)) {
                    configMap.put(conf, new DatumConfig(conf));
                }
            }
        }
        return configMap.get(conf);
    }

    public DatumConfig(Configuration conf) {
        super(conf);
    }

    @Override
    protected void load(Document document) {
        datums = new ArrayList<DatumSet>();
        Element root = document.getRootElement();
        if (root.getQName().getName().equals("datum")) {
            datums.add(new DatumSet(root));
        } else {
            List<Element> lists = root.elements();
            for (Element el : lists) {
                datums.add(new DatumSet(el));
            }
        }
    }

    @Override
    protected String getConfXmlKey() {
        return CONF_XML;
    }

    public DatumFilter getDatumFilter(org.jsoup.nodes.Document doc) {
        for (DatumSet set : datums) {
            if (set.match(doc)) {
                return set.getDatumFilter();
            }
        }
        return null;
    }

    public Iterator<Field> getFields(org.jsoup.nodes.Document doc) {
        for (DatumSet set : datums) {
            if (set.match(doc)) {
                return set.getFields();
            }
        }
        return empty.iterator();
    }

    public Field getField(org.jsoup.nodes.Document doc, String name) {
        for (DatumSet set : datums) {
            if (set.match(doc)) {
                return set.getField(name);
            }
        }
        return null;
    }
}