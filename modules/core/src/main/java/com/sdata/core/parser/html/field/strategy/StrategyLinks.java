package com.sdata.core.parser.html.field.strategy;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import net.sf.json.JSONObject;

import com.lakeside.core.utils.MapUtils;
import com.lakeside.core.utils.PatternUtils;
import com.lakeside.core.utils.StringUtils;
import com.sdata.context.parser.IParserContext;
import com.sdata.core.parser.html.field.Tags;


/**
 * @author zhufb
 *
 */
public class StrategyLinks extends StrategyField {

	protected String replace;
	public String getName() {
		return Tags.LINKS.getName();
	}
	
	public Object getData(IParserContext context, JSONObject json) {
		if(StringUtils.isEmpty(path)){
			return null;
		}
		Object obj = MapUtils.getInter(json, path);
		if (obj == null || StringUtils.isEmpty(replace)) {
			return obj;
		}
		String origin = (String) super.getSelectValue(context, null);
		if (StringUtils.isEmpty(origin)) {
			return obj;
		}
		// list
		if (obj instanceof List) {
			List<String> result = new ArrayList<String>();
			Iterator<Object> iterator = ((List) obj).iterator();
			while (iterator.hasNext()) {
				String r = PatternUtils.replaceMatchGroup(replace, origin, 1,
						StringUtils.valueOf(iterator.next()));
				if (!StringUtils.isEmpty(r) && !r.equals(origin)
						&& !result.contains(r))
					result.add(r);
			}
			return result;
		}
		// other
		String r = PatternUtils.replaceMatchGroup(replace, origin, 1,StringUtils.valueOf(obj));
		return origin.equals(r)?obj:r;
	}

	public String getReplace() {
		return replace;
	}

	public void setReplace(String replace) {
		this.replace = replace;
	}

}
