package com.sdata.solr;

import java.util.Map;

/**
 * @author zhufb
 *
 */
public class IndexControler {
	
	public static void init(IndexSite indexSite){
		if(indexSite == null) {
			return;
		}
		ServerFactory.init(indexSite);
	}
	
	public static void add(Map<String,Object> data ){
		int size = ServerFactory.size();
		for(int i=0;i<size;i++){
			IndexServer indexServer = ServerFactory.get(i);
			if(indexServer.server(data)){
				return;
			}
		}
	}
	
	public static void complete(){
		int size = ServerFactory.size();
		for(int i=0;i<size;i++){
			IndexServer indexServer = ServerFactory.get(i);
			indexServer.complete();
		}
	}
}
