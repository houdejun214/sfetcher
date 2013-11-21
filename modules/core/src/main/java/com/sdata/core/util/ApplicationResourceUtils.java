package com.sdata.core.util;

import java.io.File;
import java.net.URL;

import com.lakeside.core.utils.ClassUtils;
import com.lakeside.core.utils.FileUtils;
import com.lakeside.core.utils.PathUtils;
import com.lakeside.core.utils.StringUtils;

/**
 * 
 * 资源文件工具类
 * 
 * @author houdejun
 *
 */
public class ApplicationResourceUtils {
	
	private static String applicationRoot;
	
	private static String applicationClassPathRoot="";
	
	static {
		// get the root directory of current application
		File current = new File(".");
		String dir = current.getAbsolutePath();
		applicationRoot = PathUtils.getParentPath(dir);
		// get the root path of classpath;
		ClassLoader defaultClassLoader = ClassUtils.getDefaultClassLoader();
		URL resource = defaultClassLoader.getResource("");
		if(resource!=null){
			applicationClassPathRoot = resource.getPath();
		}
	}
	
	/**
	 * get the resource file url that is relative to the current application directory if it is a relative path
	 * 
	 * @param path
	 * @return
	 */
	public static String getResourceUrl(String path){
		File file=new File(path);
		if(file.isAbsolute()){
			return path;
		}else{
			if(!StringUtils.isEmpty(applicationClassPathRoot)){
				String url = PathUtils.getPath(applicationClassPathRoot+"/"+path);
				if(FileUtils.exist(url)){
					return url;
				}
			}
			String url = PathUtils.getPath(applicationRoot+"/"+path);
			return url;
		}
	}
	
	/**
	 * 系统首先从当前application class path 获取文件，如果文件不存在再从baseClass的资源文件中获取文件。最后从当前目录下获取文件。
	 * @param baseClass
	 * @param path
	 * @return
	 */
	public static String getResourceUrl(Class<?> baseClass,String path){
		File file=new File(path);
		if(file.isAbsolute()){
			return path;
		}else{
			if(!StringUtils.isEmpty(applicationClassPathRoot)){
				String url = PathUtils.getPath(applicationClassPathRoot+"/"+path);
				if(FileUtils.exist(url)){
					return url;
				}
			}
			URL resource =  baseClass.getResource("/"+path);
			if(resource!=null && FileUtils.exist(resource.getPath())){
				return resource.getPath();
			}
			String url = PathUtils.getPath(applicationRoot+"/"+path);
			return url;
		}
	}
}
