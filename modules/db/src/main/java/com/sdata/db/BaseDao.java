package com.sdata.db;

import java.util.Map;

/**
 *
 *
 */
public abstract class BaseDao {

    protected Collection collection;

    public BaseDao(Collection collection){
        this.collection = collection;
    }

	/**
	 * @param data
	 */
	public abstract boolean save(Map<String,Object> data);
	
	/**
	 * exists
	 * @return
	 */
	public abstract boolean isExists(Object id);
	
	/**
	 * delete
	 * @return
	 */
	public abstract void delete(Object id);
}
