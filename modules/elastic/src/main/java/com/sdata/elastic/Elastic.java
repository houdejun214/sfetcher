package com.sdata.elastic;

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

import com.framework.config.ClusterConfig;
import com.framework.config.NCluster;
import com.framework.config.NEntry;
import com.framework.config.NSouce;
import com.framework.config.NSourceIndex;
import com.framework.config.SourceConfig;
import com.lakeside.core.utils.ApplicationResourceUtils;
import com.lakeside.core.utils.FileUtils;

/**
 * @author zhufb
 *
 */
public class Elastic {

	private final String INDEX_ID = "id";
	private final int TIMES = 3;
	private TransportClient client;
	private String indexName;
	private String indexType;
	public TransportClient getClient() {
		return client;
	}

	public Elastic(String clusterName,String sourceName,String elasticMapping) {
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
		this.init(sourceName,elasticMapping);
	}
	
	private void init(String sName,String mapping){
		try {
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
