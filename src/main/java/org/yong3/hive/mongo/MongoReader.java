package org.yong3.hive.mongo;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.RecordReader;

import com.mongodb.DBCursor;
import com.mongodb.DBObject;

public class MongoReader implements RecordReader<LongWritable, MapWritable> {
	final static int BATCH_SIZE = 8192;
	MongoTable table;
	MongoSplit split;
	DBCursor cursor;
	long pos;
	String[] readColumns;

	public MongoReader(String dbHost, String dbPort, String dbName, String dbUser, String dbPasswd, 
			String colName, MongoSplit split, String[] readColumns) {
		this.table = new MongoTable(dbHost, dbPort, dbName, dbUser, dbPasswd, colName);
		this.split = split;
		this.readColumns = readColumns;

		this.cursor = table.findAll(readColumns).batchSize(BATCH_SIZE).skip(
				(int) split.getStart());
		if (!split.isLastSplit())
			this.cursor.limit((int) split.getLength());// if it's the last
		// split,it will read
		// all records since
		// $start

	}

	@Override
	public void close() throws IOException {
		if (table != null)
			table.close();
	}

	@Override
	public LongWritable createKey() {
		return new LongWritable();
	}

	@Override
	public MapWritable createValue() {
		return new MapWritable();
	}

	@Override
	public long getPos() throws IOException {
		return this.pos;
	}

	@Override
	public float getProgress() throws IOException {
		return split.getLength() > 0 ? pos / (float) split.getLength() : 1.0f;
	}

	@Override
	public boolean next(LongWritable keyHolder, MapWritable valueHolder)
			throws IOException {
		if (!cursor.hasNext()) {
			return false;
		}
		DBObject record = cursor.next();
		keyHolder.set(pos);
		for (int i = 0; i < this.readColumns.length; i++) {
			String key = readColumns[i];
			Object vObj = ("id".equals(key)) ? record.get("_id") : record
					.get(key);
			Writable value = (vObj == null) ? NullWritable.get() : new Text(
					vObj.toString());
			valueHolder.put(new Text(key), value);
		}
		pos++;
		return true;
	}

}
