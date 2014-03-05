package com.sdata.core.parser.html.util;

import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang.StringUtils;

import com.sdata.context.parser.IParserContext;

/**
 * @author zhufb
 *
 */
public class FieldInvoke {

	private static final String ACTION_TONUMBER="tonumber";
	private static final String ACTION_TOLONG="tolong";
	private static final String ACTION_TOUUID="touuid";
	private static final String ACTION_TODATE="todate";
	private static final String ACTION_SAVEIMAGE="saveImage";
	private static Map<String,InvokeMethod> methodFatory = new HashMap<String,InvokeMethod>();
	
	public static Object handle(String className,IParserContext context,Object data){
		if(StringUtils.isEmpty(className)||data == null){
			return data;
		}

		try{
			InvokeMethod im = getInvokdMethod(className, "handle");
			return  im.getMethod().invoke(im.getInstance(), new Object[]{context,data});
		}catch(Exception e){
			e.printStackTrace();
			throw new RuntimeException("invoke handle error,class "+className+",log "+ e.getMessage());
		}
	}
	
	public static boolean filter(String className,IParserContext context,Object data){
		if(StringUtils.isEmpty(className)||data == null){
			return true;
		}
		try{
			InvokeMethod im = getInvokdMethod(className, "filter");
			return  (Boolean)im.getMethod().invoke(im.getInstance(), new Object[]{context,data});
		}catch(Exception e){
			e.printStackTrace();
			throw new RuntimeException("invoke filter error,class "+className+",log "+ e.getMessage());
		}
	}
	
	public static Object action(String action,IParserContext context,Object data){
		if(StringUtils.isEmpty(action)||data == null){
			return data;
		}
		if(ACTION_SAVEIMAGE.equals(action)){
			return mediaAction(context,data);
		}
		
		String str_method = null;
		if(ACTION_TONUMBER.equals(action)){
			str_method = "toNumber";
		}else if(ACTION_TOLONG.equals(action)){
			str_method = "toLong";
		}else if(ACTION_TODATE.equals(action)){
			str_method = "toDate";
		}else if(ACTION_TOUUID.equals(action)){
			str_method = "toUUID";
		}
		
		if(StringUtils.isEmpty(str_method)){
			return data;
		}
		
		try{
			InvokeMethod im = getInvokdMethod("com.sdata.core.parser.html.action.ParserAction", str_method);
			return  im.getMethod().invoke(im.getInstance(), new Object[]{data});
		}catch(Exception e){
			throw new RuntimeException("invoke action error,action "+action+",log "+ e.getMessage());
		}
	}
	
	private static Object mediaAction(IParserContext context,Object data){
		//TODO do not save image now
		return data;
//		try{
//			InvokeMethod im = getInvokdMethod("com.sdata.core.parser.html.action.MediaAction", "saveImage");
//			return  im.getMethod().invoke(im.getInstance(), new Object[]{context,data});
//		}catch(Exception e){
//			throw new RuntimeException("invoke action error,action save image log "+ e.getMessage());
//		}
	}
	
	private static InvokeMethod getInvokdMethod(String className,String methodName) throws Exception  {
		String key = className.concat(methodName);
		if(!methodFatory.containsKey(key)){
			synchronized (methodFatory) {
				if(!methodFatory.containsKey(key)){
					Class<?> classType = Class.forName(className);
					Object classInstance = classType.newInstance();
					Method method = null;
					try{
						 method = classType.getMethod(methodName, new Class[]{IParserContext.class,Object.class});
					}catch(NoSuchMethodException e){
						 method = classType.getMethod(methodName, new Class[]{Object.class});
					}
					methodFatory.put(key, new InvokeMethod(classInstance,method));
				}
			}
		}
		return methodFatory.get(key);
	}
	
	private static class InvokeMethod {
		public InvokeMethod(Object instance,Method method){
			this.instance = instance;
			this.method = method;
		}
		private Object instance;
		private Method method;
		public Object getInstance() {
			return instance;
		}
		public Method getMethod() {
			return method;
		}
	}
	
}
