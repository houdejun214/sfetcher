package com.sdata.component.data.dao;

import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.springframework.stereotype.Repository;

import com.lakeside.data.mongo.MongoDao;
import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.sdata.core.Constants;
import com.sdata.core.data.FieldProcess;

@Repository
public class AddressDao extends MongoDao {

	private static final String addrCollection = "address";
	private DBCollection pdCollection;

	public void insert(Map<String, Object> addr, FieldProcess fieldProcess) {
		if (pdCollection == null) {
			pdCollection = this.getDBCollection(addrCollection);
		}
		String addrId = (String) addr.get(Constants.OBJECT_ID);
		if (StringUtils.isEmpty(addrId))
			throw new RuntimeException("the property _id of address is empty!");
		BasicDBObject query = new BasicDBObject(Constants.OBJECT_ID,Long.valueOf(addrId));
		BasicDBObject update = new BasicDBObject();
		addr.remove(Constants.OBJECT_ID);
		update.putAll(addr);
		pdCollection.findAndModify(query, null, null, false, new BasicDBObject("$set", update), false, true);
	}
}
