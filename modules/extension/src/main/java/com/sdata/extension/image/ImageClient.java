package com.sdata.extension.image;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.sdata.context.config.Configuration;

/**
 * @author zhufb
 *
 */
public class ImageClient {
	private ImageProducer ip;
	private final static Map<Configuration,ImageClient> clients = new HashMap<Configuration, ImageClient>();
	public static ImageClient getInstance(Configuration config){
		if(!clients.containsKey(config)){
			synchronized (clients) {
				if(!clients.containsKey(config))
					clients.put(config, new ImageClient(config));
			}
		}
		return clients.get(config);
	}
	
	private ImageClient(Configuration config){
		this.ip = new ImageProducer();
	}
		
	public void save(String source,List<String> list)  {
		if(list==null||list.size() == 0){
			return;
		}
		ip.produce(source,list);
	}

}

	

