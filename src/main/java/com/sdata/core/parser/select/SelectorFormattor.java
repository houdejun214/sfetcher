package com.sdata.core.parser.select;

import com.lakeside.core.utils.StringUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class SelectorFormattor {

	private FormatString[] fsa;

	public SelectorFormattor(String format) {
		fsa = parse(format);
	}

	public String format(Object input, PageContext context) {
		StringBuilder sb = new StringBuilder();
		for (int i = 0; i < fsa.length; i++) {
			FormatString fs = fsa[i];
			int type = fs.type();
			if(type==2){
				sb.append(fs.apply(input));
			}else{
				sb.append(fs.apply(context));
			}
		}
		return sb.toString();
	}

	private static final String formatSpecifier = "\\{([_a-zA-Z0-9]+)\\}";

	private static Pattern fsPattern = Pattern.compile(formatSpecifier);

	private FormatString[] parse(String format) {
		List<FormatString> al = new ArrayList<FormatString>();
		Matcher m = fsPattern.matcher(format);
		int i = 0;
		while (i < format.length()) {
			if (m.find(i)) {
				if (m.start() != i) {
					// Assume previous characters were fixed text
					al.add(new FixedString(format.substring(i, m.start())));
				}
				String org = m.group(0);
				String txt = m.group(1);
				if (StringUtils.isNum(txt)) {
					al.add(new ArrayFormatString(org,txt));
				} else {
					al.add(new VariableFormatString(org,txt));
				}
				i = m.end();
			} else {
				al.add(new FixedString(format.substring(i)));
				break;
			}
		}
		return al.toArray(new FormatString[0]);
	}
	
	
	private static interface FormatString {
		String apply(Object input);
		int type();
	}

	private static class FixedString implements FormatString {
		
		private String s;
		
		FixedString(String s) {
			this.s = s;
		}
		public String apply(Object arg) {
			return s;
		}
		public int type() {
			return 0;
		}
	}

	private static class VariableFormatString implements FormatString {
		private String orgformat;
		private String variableName;
		VariableFormatString(String format,String name){
			this.orgformat = format;
			this.variableName = name;
		}
		public String apply(Object arg) {
			Object variable=null;
			if(arg instanceof PageContext){
                PageContext context = (PageContext)arg;
				variable = context.getVariable(variableName);
			}
			if(variable==null){
				return "";
			}else{
				return variable.toString();
			}
		}
		public int type() {
			return 1;
		}
	}

	private static class ArrayFormatString implements FormatString {
		private int index=-1;
		private String orgformat;
		
		ArrayFormatString(String format,String txt){
			this.orgformat = format;
			this.index = StringUtils.toInt(txt);
		}
		public String apply(Object arg) {
			Object variable=null;
			List list = null;
			if(arg == null){
				return orgformat;
			}else if(!(arg instanceof List)){
				list = new ArrayList();
				list.add(arg);
			}else{
				list = (List)arg;
			}
			if(list!=null&&list.size()>index){
				variable = list.get(index);
			}
			if(variable==null){
				return orgformat;
			}else{
				return variable.toString();
			}
		}
		public int type() {
			return 2;
		}

	}
}
