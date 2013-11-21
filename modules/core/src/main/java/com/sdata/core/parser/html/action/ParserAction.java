package com.sdata.core.parser.html.action;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.lakeside.core.utils.StringUtils;
import com.lakeside.core.utils.UUIDUtils;
import com.lakeside.core.utils.time.DateFormat;

/**
 * @author zhufb
 *
 */
public class ParserAction {
	
	private static final Pattern NumberPattern=Pattern.compile("(-?[1-9]\\d*\\,?\\d*\\.?\\d+)");
	
	public Object toLong(Object obj){
		if(obj == null){
			return obj;
		}
		String str = StringUtils.valueOf(obj);
		if(!StringUtils.isNum(str)){
			return obj;
		}
		return Long.valueOf(str);
	}

	public Object toDate(Object obj){
		return DateFormat.changeStrToDate(obj);
	}
	
	public Object toUUID(Object obj){
		if(obj!=null){
			return UUIDUtils.getMd5UUID(obj.toString());
		}
		return null;
	}
	
	public Object toNumber(Object data) {
		if(data!=null){
			String _str = data.toString();
			Matcher matcher = NumberPattern.matcher(_str);
			if(matcher.find()){
				String group = matcher.group(1);
				group = group.replaceAll(",", "");
				if(group.contains(".")){
					return Float.parseFloat(group);
				}else{
					Long longVal = Long.valueOf(group);
					if(Integer.MIN_VALUE<longVal && longVal<Integer.MAX_VALUE){
						return longVal.intValue();
					}
					return longVal;
				}
			}
		}
		return null;
	}
}
