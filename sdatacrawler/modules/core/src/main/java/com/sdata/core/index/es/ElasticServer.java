package com.sdata.core.index.es;

import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.mapper.MergeMappingException;

import com.lakeside.core.utils.ApplicationResourceUtils;
import com.lakeside.core.utils.FileUtils;
import com.nus.next.config.ClusterConfig;
import com.nus.next.config.NCluster;
import com.nus.next.config.NEntry;
import com.nus.next.config.NSouce;
import com.nus.next.config.NSourceIndex;
import com.nus.next.config.SourceConfig;
import com.sdata.core.Constants;
import com.sdata.core.CrawlAppContext;

public class ElasticServer {

	private final String INDEX_ID = "id";
	private final int TIMES = 3;
	private TransportClient client;
	private String indexName;
	private String indexType;
	public TransportClient getClient() {
		return client;
	}

	public ElasticServer(String clusterName) {
		ClusterConfig clusterConfig = ClusterConfig.getInstance();
		NCluster cluster = clusterConfig.getESCluster(clusterName);
		Settings settings = ImmutableSettings.settingsBuilder()   
				.put("client.transport.sniff", true)
                .put("client.transport.ping_timeout","10s")
                .put("cluster.name", clusterName).build(); 
		
		client = new TransportClient(settings);
		
		for(NEntry entry:cluster.getList()){
			client.addTransportAddress(new InetSocketTransportAddress(entry.getHost(), entry.getPort()));
		}
		this.init();
	}
	
	private void init(){
		try {
			String sName = CrawlAppContext.conf.get(Constants.SOURCE);
			if(StringUtils.isEmpty(sName)){
				sName = CrawlAppContext.state.getCrawlName();
			}
			String mapping = CrawlAppContext.conf.get("ElasticMapping");
			if(StringUtils.isEmpty(sName)){
		    	throw new RuntimeException("get NSouce error,sourceName is null:"+sName);
			}
		    NSouce source = SourceConfig.getInstance().getSource(sName);
		    if(source == null){
		    	throw new RuntimeException("get NSouce error,cant find source:"+sName);
		    }
		    NSourceIndex sourceIndex = source.getSourceIndex();
		    if(sourceIndex == null){
		    	throw new RuntimeException("get sourceIndex error,cant find source:"+sName);
		    }
		    
		    indexName = sourceIndex.getIndexName();
		    indexType = sourceIndex.getTypeName();
			final String resourceUrl = ApplicationResourceUtils.getResourceUrl(mapping);
			String content = FileUtils.readFileToString(resourceUrl);
			content = content.replace("ugc", sName);
			if(!this.client.admin().indices().prepareExists(indexName).execute().get().exists()){
				// create index
				CreateIndexRequest request = Requests.createIndexRequest(indexName);
				request.mapping(sName, content);
				this.client.admin().indices().create(request).get();
			}
		} catch (MergeMappingException e) {
			System.out.println("excpetion but ignore when update metadate: "+ e.getDetailedMessage());
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}
	
	public void save(Map<String, Object> data){
		Object id = data.remove(INDEX_ID);
		if(id == null||"".equals(id)){
			throw new RuntimeException("Elastic Server save error, 'id' in index data is null!");
		}
		IndexRequest request = new IndexRequest(indexName)
		.type(indexType)
		.id(id.toString())
		.source(build(data));
		String message = "";
		// if failed try three times
		int time = 0;
		while(time<TIMES){
			try {
				client.index(request).get();
				return;
			} catch (Exception e) {
				message = e.getMessage();
				time++;
				continue;
			}
		}
		throw new RuntimeException("Elastic Server save exception:"+message);
	}
	
	private XContentBuilder build(final Map<String, Object> data) {
		try {
			return XContentFactory.jsonBuilder().map(data);
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}
}
