package org.yong3.hive.mongo;

import java.net.UnknownHostException;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.Mongo;
import com.mongodb.MongoException;

public class MongoTable {
	private DB db;
	private DBCollection collection;

	public MongoTable(String host, String port, String dbName,
			String collectionName) {
		try {
			this.db = new Mongo(host, Integer.valueOf(port)).getDB(dbName);
			this.collection = db.getCollection(collectionName);
		} catch (UnknownHostException e) {
			e.printStackTrace();
		} catch (MongoException e) {
			e.printStackTrace();
		}
	}

	public void save(BasicDBObject dbo) {
		this.collection.save(dbo);
	}

	public void close() {
		if (db != null) {
			db.getMongo().close();
		}
	}

	public long count() {
		return (this.collection != null) ? this.collection.count() : 0;
	}

	public DBCursor findAll(String[] fields) {
		DBObject qFields = new BasicDBObject();
		for (String field : fields) {
			qFields.put(field, 1);
		}

		return this.collection.find(qFields);
	}

}
