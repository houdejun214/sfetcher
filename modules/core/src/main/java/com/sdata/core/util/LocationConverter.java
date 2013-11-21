package com.sdata.core.util;


import org.apache.commons.beanutils.converters.AbstractConverter;

import com.lakeside.core.utils.StringUtils;
import com.sdata.core.Location;

public class LocationConverter extends AbstractConverter{

	@Override
	protected Object convertToType(Class type, Object value) throws Throwable {
		String str_value="";
		if(value!=null){
			str_value=value.toString();
		}
		if(StringUtils.isEmpty(str_value)){
			return null;
		}
		//1.216332,103.633058
		String[] values = str_value.split(",");
		if(value==null || values.length!=2){
			return null;
		}
		return new Location(values[0],values[1]);
	}

	@Override
	protected Class getDefaultType() {
		// TODO Auto-generated method stub
		return null;
	}
	
	
}
