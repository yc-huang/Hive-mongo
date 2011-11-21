package org.yong3.hive.mongo;

import static org.yong3.hive.mongo.ConfigurationUtil.copyGDataProperties;

import java.util.Map;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.HiveMetaHook;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.ql.metadata.HiveStorageHandler;
import org.apache.hadoop.hive.ql.plan.TableDesc;
import org.apache.hadoop.hive.serde2.SerDe;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.OutputFormat;

@SuppressWarnings("unchecked")
public class MongoStorageHandler implements HiveStorageHandler {
	private Configuration mConf = null;
	
	public MongoStorageHandler(){
	}

	@Override
	public void configureTableJobProperties(TableDesc tableDesc,
			Map<String, String> jobProperties) {
		Properties properties = tableDesc.getProperties();
		
	    copyGDataProperties(properties, jobProperties);
	}

	@Override
	public Class<? extends InputFormat> getInputFormatClass() {
		return MongoInputFormat.class;
	}

	@Override
	public HiveMetaHook getMetaHook() {
		return new DummyMetaHook();
	}


	@Override
	public Class<? extends OutputFormat> getOutputFormatClass() {
		return MongoOutputFormat.class;
	}

	@Override
	public Class<? extends SerDe> getSerDeClass() {
		return MongoSerDe.class;
	}

	@Override
	public Configuration getConf() {
		return this.mConf;
	}

	@Override
	public void setConf(Configuration conf) {
		this.mConf = conf;
	}

	private static class DummyMetaHook implements HiveMetaHook {

		@Override
		public void commitCreateTable(Table arg0) throws MetaException {
			// TODO Auto-generated method stub

		}

		@Override
		public void commitDropTable(Table arg0, boolean arg1)
				throws MetaException {
			// TODO Auto-generated method stub

		}

		@Override
		public void preCreateTable(Table arg0) throws MetaException {
			// TODO Auto-generated method stub

		}

		@Override
		public void preDropTable(Table arg0) throws MetaException {
			// TODO Auto-generated method stub

		}

		@Override
		public void rollbackCreateTable(Table arg0) throws MetaException {
			// TODO Auto-generated method stub

		}

		@Override
		public void rollbackDropTable(Table arg0) throws MetaException {
			// TODO Auto-generated method stub

		}

	}
}
