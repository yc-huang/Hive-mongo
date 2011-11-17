package org.yong3.hive.mongo;

import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.hive.ql.exec.FileSinkOperator.RecordWriter;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Writable;

import com.mongodb.BasicDBObject;

public class MongoWriter implements RecordWriter {
	String host, port, dbName;
	MongoTable table;

	public MongoWriter(String host, String port, String dbName, String colName) {
		this.host = host;
		this.port = port;
		this.dbName = dbName;
		this.table = new MongoTable(host, port, this.dbName, colName);
	}

	@Override
	public void close(boolean abort) throws IOException {
		if (table != null)
			table.close();
	}

	@Override
	public void write(Writable w) throws IOException {
		MapWritable map = (MapWritable) w;
		BasicDBObject dbo = new BasicDBObject();
		for (final Map.Entry<Writable, Writable> entry : map.entrySet()) {
			 System.err.println("Write: key=" + entry.getKey().toString() +
			 ", val=" + entry.getValue().toString());
			String key = entry.getKey().toString();
			if ("id".equals(key))
				key = "_id";
			dbo.put(key, entry.getValue().toString());
		}
		table.save(dbo);
	}

}
