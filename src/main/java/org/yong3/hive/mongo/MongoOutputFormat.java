package org.yong3.hive.mongo;

import java.io.IOException;
import java.util.Properties;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.Row;
import org.apache.hadoop.hive.ql.exec.FileSinkOperator.RecordWriter;
import org.apache.hadoop.hive.ql.io.HiveOutputFormat;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputFormat;
import org.apache.hadoop.util.Progressable;

import static org.yong3.hive.mongo.ConfigurationUtil.*;

@SuppressWarnings("deprecation")
public class MongoOutputFormat implements OutputFormat<NullWritable,Row>,
HiveOutputFormat<NullWritable, Row>{

	@Override
	public RecordWriter getHiveRecordWriter(JobConf conf,
		      Path finalOutPath,
		      Class<? extends Writable> valueClass,
		      boolean isCompressed,
		      Properties tableProperties,
		      Progressable progress) throws IOException {
		return new MongoWriter(getDBHost(conf), getDBPort(conf), getDBName(conf), getCollectionName(conf));
		
	}

	@Override
	public void checkOutputSpecs(FileSystem arg0, JobConf conf)
			throws IOException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public org.apache.hadoop.mapred.RecordWriter<NullWritable, Row> getRecordWriter(
			FileSystem arg0, JobConf arg1, String arg2, Progressable arg3)
			throws IOException {
		throw new RuntimeException("Error: Hive should not invoke this method.");
	}

}
