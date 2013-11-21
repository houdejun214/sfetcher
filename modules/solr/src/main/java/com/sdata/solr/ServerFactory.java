package com.sdata.solr;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * @author zhufb
 *
 */
public class ServerFactory {
	
	private static List<IndexServer> list = new ArrayList<IndexServer>();
	public static void init(IndexSite indexSite){
		if(indexSite == null) {
			return;
		}
		Iterator<IndexSource> iterator = indexSite.getList().iterator();
		while(iterator.hasNext()){
			IndexSource next = iterator.next();
			list.add(new IndexServer(next));
		}
	}
	public static int size(){
		return list.size();
	}
	public static IndexServer get(int i){
		return list.get(i);
	}
	public static List<IndexServer> getServerList(){
		return list;
	}
}
