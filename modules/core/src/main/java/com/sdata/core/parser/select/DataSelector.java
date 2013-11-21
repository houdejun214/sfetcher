package com.sdata.core.parser.select;

import java.util.Collection;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import net.sf.json.JSONArray;

import com.sdata.core.parser.html.context.IParserContext;



public abstract class DataSelector {
	
	public static Pattern SELECTOR_REGX = Pattern.compile("^~/(.*)/$");
	
	public static Pattern SELECTOR_ARRAYS = Pattern.compile("^\\[([0-9]*?)\\]$");
	
	public static Pattern SELECTOR_FORMAT = Pattern.compile("^\\{(.*)\\}$");
	
	public static Pattern SELECTOR_ATTRI = Pattern.compile("^\\[([a-zA-Z0-9\\-\\_]*?)\\]$");
	
	public static Pattern SELECTOR_TEXT = Pattern.compile("^[tT]e?xt$");
	
	public static Pattern SELECTOR_HTML = Pattern.compile("^[h|H]tml$");
	
	public static Pattern SELECTOR_LINK = Pattern.compile("^[l|L]ink$");
	
	public static Pattern SELECTOR_LINKLIST = Pattern.compile("^[l|L]inks$");
	
	public static Pattern SELECTOR_FILTER = Pattern.compile("^:\\[(.*)\\]$");
	
	protected String selector;
	
	private DataSelector next;
	
	protected IParserContext context;
	
	public Object select(Object inputObject){
		Object result = this.selectObject(inputObject);
		DataSelector _next = next;
		while(result!=null && _next!=null){
			_next.setContext(context);
			result = _next.selectObject(result);
			_next = _next.getNext();
		}
		return result;
	}
	
	/**
	 * this method must be override by each implementation 
	 * 
	 * @param inputObject
	 * @return
	 */
	abstract Object selectObject(Object inputObject);

	public DataSelector getNext() {
		return next;
	}

	public void setNext(DataSelector next) {
		next.setContext(this.context);
		this.next = next;
	}
	
	public void setContext(IParserContext context) {
		this.context = context;
	}

	protected Object getFirstObjectIfList(Object input){
		return getIndexAtObjectIfList(input,0);
	}
	
	protected Object getIndexAtObjectIfList(Object input,int index){
		if(input instanceof List){
			List list = (List)input;
			if(list.size()>index){
				return list.get(index);
			}
		}
		return input;
	}
	
	public static boolean isArray( Object obj ) {
	      if( (obj != null && obj.getClass()
	            .isArray()) || (obj instanceof Collection) || (obj instanceof JSONArray) ){
	         return true;
	      }
	      return false;
	   }
	
	public static boolean match(Pattern pat,String input){
		Matcher matcher = pat.matcher(input);
		return matcher.matches();
	}
}
