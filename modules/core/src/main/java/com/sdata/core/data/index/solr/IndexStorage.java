package com.sdata.core.data.index.solr;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.commons.httpclient.util.DateUtil;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.apache.solr.common.SolrInputDocument;

import com.lakeside.core.utils.StringUtils;
import com.sdata.core.Configuration;

/**
 * @author zhufb
 *
 */
public class IndexStorage {
	private final int TIMES = 3;
	private SolrServer server;
	public IndexStorage(String core,Configuration config){
		String serverUrl = config.get("solrServer").concat("/").concat(core);
		server = new HttpSolrServer(serverUrl);
	}

	IndexStorage(SolrServer server){
		this.server = server;
	}
	
	public void index(List<Map<String,Object>> datas) {
		if(datas == null||datas.size() == 0) return;
		Collection<SolrInputDocument> docs = new ArrayList<SolrInputDocument>();
		Iterator<Map<String, Object>> iterator = datas.iterator();
		while(iterator.hasNext()){
			SolrInputDocument doc = this.getSolrDoc(iterator.next());
			docs.add(doc);
		}
		int times = 0;
		String error = null;
		while(true){
			try {
				server.add(docs);
				server.commit();
				return;
			} catch (SolrServerException e) {
				error = e.getMessage();
				if(error.contains("maxWarmingSearchers")){
					System.out.println(error);
					continue;
				}
				times++;
			} catch (IOException e) {
				times++;
				error = e.getMessage();
			}finally{
				if(times>=TIMES){
					return;
				}
			}
		}
	}
	
	public void delete(List<String> ids) {
		int times = 0;
		StringBuffer error = new StringBuffer();
		while(true){
			try {
				server.deleteById(ids);
				server.commit();
				return;
			} catch (SolrServerException e) {
				times++;
				error.append("times:").append(times).append(",error:").append(e.getMessage());
			} catch (IOException e) {
				times++;
				error.append("times:").append(times).append(",error:").append(e.getMessage());
			}finally{
				if(times>=TIMES){
					throw new RuntimeException(error.toString());
				}
			}
		}
	}

	public void delete(String id) {
		List<String> list = new ArrayList<String>();
		list.add(id);
		this.delete(list);
	}

	private SolrInputDocument getSolrDoc(Map<String,Object> data){
		SolrInputDocument  doc = new SolrInputDocument ();
		Iterator<String> iterator = data.keySet().iterator();
		while(iterator.hasNext()){
			String key = iterator.next();
			Object value = data.get(key);
			if(value instanceof Date){
				String datestr = DateUtil.formatDate((Date)value, "yyyy-MM-dd'T'HH:mm:ss'Z'");
				value = StringUtils.isEmpty(datestr)?null:datestr;
			}else if(value instanceof String){
				value = StringUtils.isEmpty((String)value)?null:value;
			}
			//filter
			value = SolrDataFilter.filter(key, value);
			if(value != null){
				doc.addField(key, value);
			}
		}
		return doc;
	}

}
