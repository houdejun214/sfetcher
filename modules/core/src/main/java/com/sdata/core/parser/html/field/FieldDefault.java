package com.sdata.core.parser.html.field;

import org.jsoup.nodes.Element;

import com.lakeside.core.utils.StringUtils;
import com.sdata.context.parser.IParserContext;
import com.sdata.core.parser.html.util.FieldInvoke;
import com.sdata.core.parser.select.DataSelector;
import com.sdata.core.parser.select.DataSelectorPipleBuilder;

/**
 * @author zhufb
 *
 */
public class FieldDefault {
	
	protected String ref;
	protected String value;
	protected String selector;
	protected DataSelector dataSelector;
	protected String action;
	protected String handler;
	protected String filter;

	protected Object refine(IParserContext context,Object data){
		//filter
		if(data == null||"".equals(data)){
			return null;
		}
		if(!this.filte(context,data)){
			return null;
		}
		//first action and then handle
		return this.handle(context,this.action(context,data));
	}

	protected Object getSelectValue(IParserContext context,Element doc){
		Object result = null;
		if(!StringUtils.isEmpty(value)){
			result =  value;
		}else if(!StringUtils.isEmpty(ref)){
			result = context.getVariable(ref);
		}else if(!StringUtils.isEmpty(selector)){
			dataSelector.setContext(context);
			result = dataSelector.select(doc);
		}
		return result;
	}
	
	protected boolean filte(IParserContext context,Object data){
		return FieldInvoke.filter(this.filter,context, data);
	}
	
	protected Object handle(IParserContext context,Object data){
		return	FieldInvoke.handle(this.handler,context, data);
	}

	protected Object action(IParserContext context,Object data){
		return	FieldInvoke.action(this.action, context,data);
	}

	public String getAction() {
		return action;
	}

	public void setAction(String action) {
		this.action = action;
	}

	public String getHandler() {
		return handler;
	}

	public void setHandler(String handler) {
		this.handler = handler;
	}

	public String getFilter() {
		return filter;
	}

	public void setFilter(String filter) {
		this.filter = filter;
	}

	public String getSelector() {
		return selector;
	}

	public String getRef() {
		return ref;
	}

	public void setRef(String ref) {
		this.ref = ref;
	}

	public String getValue() {
		return value;
	}

	public void setValue(String value) {
		this.value = value;
	}
	public void setSelector(String selector) {
		this.selector = selector;
		this.dataSelector = DataSelectorPipleBuilder.build(selector);
	}
	
}
