package com.sdata.hot.fetcher;

import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.eclipse.jdt.core.dom.Modifier;

import com.lakeside.core.utils.ClassUtils;
import com.lakeside.core.utils.MapUtils;
import com.sdata.core.Configuration;
import com.sdata.core.FetchDatum;
import com.sdata.hot.Hot;
import com.sdata.hot.HotConstants;
import com.sdata.hot.Source;

/**
 * @author zhufb
 *
 */
public class HotFetcherLookup {


	private final static String TypeMethod = "type";
	private final static String SourceMethod = "source";
	
	private static Map<Hot,List<Class<? extends HotBaseFetcher>>> listFetcherMap = new HashMap<Hot,List<Class<? extends HotBaseFetcher>>>();
	private static Map<String,Class<? extends HotBaseFetcher>> datumFetcherMap = new HashMap<String,Class<? extends HotBaseFetcher>>();
	private static ThreadLocal<Map<String,HotBaseFetcher>> localFetchers = new ThreadLocal<Map<String,HotBaseFetcher>>();

	static{
		if(datumFetcherMap.size() == 0){
			synchronized (datumFetcherMap) {
				if(datumFetcherMap.size() ==0){
					Map<String,Class<? extends HotBaseFetcher>> map = new HashMap<String,Class<? extends HotBaseFetcher>>();
					Set<Class<? extends HotBaseFetcher>> subClass = ClassUtils.getSubClass(HotBaseFetcher.class, "com.sdata.hot.fetcher");
					for(Class<? extends HotBaseFetcher> sub:subClass){
						if(Modifier.isAbstract(sub.getModifiers())){
							continue;
						}
						//get type
						Hot t = type(sub);
						//get source
						Source s = source(sub);
						if(t==null||s==null){
							continue;
						}
						// for fetch one datum using fetcher map
						String key = key(t.getValue(),s.getValue());
						if(StringUtils.isEmpty(key)){
							continue;
						}
						map.put(key,sub);

						//for fetch datum list using fetcher map
						List<Class<? extends HotBaseFetcher>> list = listFetcherMap.get(t);
						if(list == null){
							list = new ArrayList<Class<? extends HotBaseFetcher>>();
						}
						list.add(sub);
						listFetcherMap.put(t, list);
					}
					datumFetcherMap = map;
				}
			}
		}
	}
	
	/**
	 * 根据conf和datum获取对应的fetcher
	 * 
	 * @param conf
	 * @param datum
	 * @return
	 */
	public static List<HotBaseFetcher> getFetcher(Configuration conf,Hot type){
		List<HotBaseFetcher> result = new ArrayList<HotBaseFetcher>();
		List<Class<? extends HotBaseFetcher>> list = getListClass(type);
		if(list == null){
			throw new RuntimeException("no fetcher class with hot type  "+type);
		}
		for(Class<? extends HotBaseFetcher> cls:list){
			result.add((HotBaseFetcher)newInstance(conf,cls));
		}
		return result;
	}
	
	/**
	 * get list fetcher with type
	 * 
	 * @param type
	 * @return
	 */
	private static List<Class<? extends HotBaseFetcher>> getListClass(Hot type){
		if(!Hot.All.equals(type)){
			return listFetcherMap.get(type);
		}
		List<Class<? extends HotBaseFetcher>> all = new ArrayList<Class<? extends HotBaseFetcher>>();
		for(List<Class<? extends HotBaseFetcher>> l:listFetcherMap.values()){
			all.addAll(l);
		}
		return all;
	}
	
	/**
	 * 根据conf和datum获取对应的fetcher
	 * 
	 * @param conf
	 * @param datum
	 * @return
	 */
	public static HotBaseFetcher getFetcher(Configuration conf,FetchDatum datum){
		Integer t = MapUtils.getInt(datum.getMetadata(), HotConstants.TYPE);
		if(t == null){
			throw new RuntimeException("Fetch Datum no field "+ HotConstants.TYPE);
		}
		String s = MapUtils.getString(datum.getMetadata(), HotConstants.SOURCE);
		if(StringUtils.isEmpty(s)){
			throw new RuntimeException("Fetch Datum no field "+ HotConstants.SOURCE);
		}
		String key = key(t,s);
		Map<String, HotBaseFetcher> pool = localFetchers.get();
		if(pool == null){
			pool = new HashMap<String,HotBaseFetcher>();
			localFetchers.set(pool);
		}
		
		HotBaseFetcher fetcher = pool.get(key);
		if(fetcher == null){
			Class<? extends HotBaseFetcher> cls = datumFetcherMap.get(key);
			if(cls == null){
				throw new RuntimeException("No type_source "+ key +" HotBaseFetcher subclass exists in class... ");
			}
			fetcher = (HotBaseFetcher)newInstance(conf,cls);
		}
		pool.put(key, fetcher);
		return fetcher;
	}
	
	/**get HotBaseFetcher map key
	 * 
	 * @param cls
	 * @return
	 */
	private static Hot type(Class<?> cls){
		Object instance = newInstance(cls);
		try {
			Method m = cls.getMethod(TypeMethod);
			return (Hot)m.invoke(instance);
		} catch (Exception e) {
			throw new RuntimeException("No "+ TypeMethod +" method exists in class"+cls,e);
		}
	}
	
	/**get HotBaseFetcher map key
	 * 
	 * @param cls
	 * @return
	 */
	private static Source source(Class<?> cls){
		Object instance = newInstance(cls);
		try {
			Method m = cls.getMethod(SourceMethod);
			return (Source)m.invoke(instance);
		} catch (Exception e) {
			throw new RuntimeException("No "+ SourceMethod +" method exists in class"+cls,e);
		}
	}
	
	/**
	 * get HotBaseFetcher map key
	 * 
	 * @param t
	 * @param s
	 * @return
	 */
	private static String key(Integer t,String s){
		StringBuffer sb = new StringBuffer();
		sb.append(t).append("_").append(s);
		return sb.toString();
	}
	
	
	/**
	 * 实例化一个类
	 * 
	 * @param conf
	 * @param id
	 * @param cls
	 * @return
	 */
	private static Object newInstance(Class cls){
		try {
			Constructor constructor = cls.getConstructor();
			Object newInstance = constructor.newInstance();
			return newInstance;
		} catch (NoSuchMethodException e) {
			throw new RuntimeException("class must have no paramter constructor "+cls,e);
		} catch (Exception e) {
			throw new RuntimeException("new instance with no paramter constructor failed "+cls,e);
		} 
	}
	
	/**
	 * 实例化一个类
	 * 
	 * @param conf
	 * @param id
	 * @param cls
	 * @return
	 */
	private static Object newInstance(Configuration conf,Class cls){
		try {
			Constructor constructor = cls.getConstructor(Configuration.class);
			Object newInstance = constructor.newInstance(conf);
			return newInstance;
		} catch (Exception e) {
			throw new RuntimeException("new instance "+cls,e);
		}
	}
}
