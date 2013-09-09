package org.yong3.hive.mongo;

import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.hive.ql.exec.FileSinkOperator.RecordWriter;
import org.apache.hadoop.hive.serde2.io.ByteWritable;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.hive.serde2.io.ShortWritable;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;

import com.mongodb.BasicDBObject;

public class MongoWriter implements RecordWriter {
	MongoTable table;

	public MongoWriter(String host, String port, String dbName, String dbUser, String dbPasswd, String colName) {
		this.table = new MongoTable(host, port, dbName, dbUser, dbPasswd, colName);
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
			// System.err.println("Write: key=" + entry.getKey().toString()
			// + ", val=" + entry.getValue().toString());
			String key = entry.getKey().toString();
//			if ("id".equals(key)) {
//				key = "_id";
//			}
			dbo.put(key, getObjectFromWritable(entry.getValue()));
		}
		table.save(dbo);
	}

	private Object getObjectFromWritable(Writable w) {
		if (w instanceof IntWritable) {
			// int
			return ((IntWritable) w).get();
		} else if (w instanceof ShortWritable) {
			// short
			return ((ShortWritable) w).get();
		} else if (w instanceof ByteWritable) {
			// byte
			return ((ByteWritable) w).get();
		} else if (w instanceof BooleanWritable) {
			// boolean
			return ((BooleanWritable) w).get();
		} else if (w instanceof LongWritable) {
			// long
			return ((LongWritable) w).get();
		} else if (w instanceof FloatWritable) {
			// float
			return ((FloatWritable) w).get();
		} else if (w instanceof DoubleWritable) {
			// double
			return ((DoubleWritable) w).get();
		}else if (w instanceof NullWritable) {
			//null
			return null;
		} else {
			// treat as string
			return w.toString();
		}

	}

}
