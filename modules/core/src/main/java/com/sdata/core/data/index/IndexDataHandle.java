package com.sdata.core.data.index;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.lakeside.core.utils.StringUtils;


/**
 * @author zhufb
 *
 */
public class IndexDataHandle {
	private final static String GEO = "geo";
	private final static String GEO_LAT = "geo.lat";
	private final static String GEO_LNG = "geo.lng";

	private final static String TITLE = "title";
	private final static String TITLE_UNAME = "title.uname";
	private final static String TITLE_VNAME = "title.vname";
	
	private final static String IMAGES = "img_s";
	private final static String IMAGES_PATH = "url";
	private final static int MAX_IMAGE_COUNT = 1;
	private static Pattern compile = Pattern.compile("(-?\\d+.?\\d+ *, *-?\\d+.?\\d+)");
	
	//geo order by latitude,longitude. 
	public static void handle(Map<String,Object> result,String index,Object value){
		if(index==null||value==null) return;
		if(GEO.equals(index)){
			handleGeo(result,index,value);
		}else if(GEO_LAT.equals(index)){
			handleGeoLatitude(result, index, value);
		}else if(GEO_LNG.equals(index)){
			handleGeoLongitude(result, index, value);
		}else if(IMAGES.equals(index)){
			handleImages(result, index, value);
		}else if(TITLE_UNAME.equals(index)){
			handleFoursquareUname(result, index, value);
		}else if(TITLE_VNAME.equals(index)){
			handleFoursquareVname(result, index, value);
		}else{
			putIndexData(result, index, value);
		}
	}

	private static void handleFoursquareUname(Map<String, Object> result,
			String index, Object value) {
		StringBuffer title = new StringBuffer();
		title.append(value);
		if(result.containsKey(TITLE)){
			title.append(" checked in at ").append(result.get(TITLE));
		}
		result.put(TITLE, title.toString());
	}
	
	private static void handleFoursquareVname(Map<String, Object> result,
			String index, Object value) {
		StringBuffer title = new StringBuffer();
		if(result.containsKey(TITLE)){
			title.append(result.get(TITLE)).append(" checked in at ");
		}
		title.append(value);
		result.put(TITLE, title.toString());
	}

	private static void handleGeo(Map<String,Object> result,String index,Object value){
		Matcher matcher = compile.matcher(value.toString());
    	if(matcher.find()){
    		try{
	    		String _value = matcher.group();
	    		_value = _value.replaceAll(" ", "");
	    		String[] array = _value.split(",");
	    		if(array.length!=2) {
	    			return;
	    		}
	    		double lat = Double.parseDouble(array[0]);
	    		double lng = Double.parseDouble(array[1]);
	    		if(!(checkLatitude(lat)&&checkLongitude(lng))){
	    			return;
	    		}
	    		value = _value;
    		}catch(Exception e){
    			e.printStackTrace();
    			return;
    		}
    	}else{
    		if(!StringUtils.isEmpty(value.toString())){
        		System.out.println("index data handle error,can not find geo value:"+value.toString());
    		}
    		return;
    	}
    	result.put(index, value);
	}
	
	private static void handleGeoLatitude(Map<String,Object> result,String index,Object value){
		if(!checkLatitude(Double.parseDouble(value.toString()))){
			return;
		}
		StringBuffer geo = new StringBuffer();
		geo.append(value);
		if(result.containsKey(GEO)){
			geo.append(",").append(result.get(GEO));
		}
		result.put(GEO, geo.toString());
	}
	
	private static void handleGeoLongitude(Map<String,Object> result,String index,Object value){
		if(!checkLongitude(Double.parseDouble(value.toString()))){
			return;
		}
		StringBuffer geo = new StringBuffer();
		if(result.containsKey(GEO)){
			geo.append(result.get(GEO)).append(",");
		}
		geo.append(value);
		result.put(GEO, geo.toString());
	}

	private static void handleImages(Map<String,Object> result,String index,Object value){
		String str = null;
		if(value instanceof Map){
			str = String.valueOf(((Map<?, ?>) value).get(IMAGES_PATH));
		}else if(value instanceof List){
			StringBuffer sb = new StringBuffer();
			Iterator<?> iterator = ((List<?>) value).iterator();
			int count = 0;
			while(iterator.hasNext()&&count<MAX_IMAGE_COUNT){
				Object next = iterator.next();
				if(next instanceof String){
					sb.append(next).append(",");
				}else if(next instanceof Map){
					sb.append(((Map<?, ?>)next).get(IMAGES_PATH)).append(",");
				}
				count++;
			}
			if(iterator.hasNext()){
				result.put("more_b", true);
			}
			str = sb.length()==0?sb.toString():sb.substring(0, sb.length()-1);
		}else if(value instanceof String){
			str = value.toString();
		}else{
			return;
		}
		result.put(IMAGES, str);
	}
	
	private static boolean checkLatitude(double latitude){
		boolean check = true;
		if(!(latitude>=-90.0&&latitude<=90.0)){
			check = false;
		}
		return check;
	}

	private static boolean checkLongitude(double latitude){
		boolean check = true;
		if(!(latitude>=-180.0&&latitude<=180.0)){
			check = false;
		}
		return check;
	}
	
	private static void putIndexData(Map<String,Object> result,String index,Object value){
		Object object = result.get(index);
		if(object!=null&&!"".equals(object)){
			value = value +" "+ object.toString();
		}
		result.put(index, value);
	}
}
