package com.sdata.db;

import java.util.Map;

/**
 * @author zhufb
 *
 */
public interface BaseDao {
	
	/**
	 * @param data
	 */
	public void save(Map<String,Object> data);
	
	/**
	 * exists
	 * 
	 * @param count
	 * @return
	 */
	public boolean isExists(Object id);
	
	/**
	 * delete
	 * 
	 * @param count
	 * @return
	 */
	public void delete(Object id);
}
