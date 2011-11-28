package org.yong3.hive.mongo;

import static org.yong3.hive.mongo.ConfigurationUtil.COLLECTION_NAME;
import static org.yong3.hive.mongo.ConfigurationUtil.DB_HOST;
import static org.yong3.hive.mongo.ConfigurationUtil.DB_NAME;
import static org.yong3.hive.mongo.ConfigurationUtil.DB_PORT;
import static org.yong3.hive.mongo.ConfigurationUtil.copyMongoProperties;

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

	public MongoStorageHandler() {
	}

	@Override
	public void configureTableJobProperties(TableDesc tableDesc,
			Map<String, String> jobProperties) {
		Properties properties = tableDesc.getProperties();
		copyMongoProperties(properties, jobProperties);
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

	private class DummyMetaHook implements HiveMetaHook {

		@Override
		public void commitCreateTable(Table tbl) throws MetaException {
			// nothing to do...
		}

		@Override
		public void commitDropTable(Table tbl, boolean deleteData)
				throws MetaException {
			if (!deleteData) {
				// nothing to do...
			} else {
				String dbHost = tbl.getParameters().get(DB_HOST);
				String dbPort = tbl.getParameters().get(DB_PORT);
				String dbName = tbl.getParameters().get(DB_NAME);
				String dbCollection = tbl.getParameters().get(COLLECTION_NAME);
				MongoTable table = new MongoTable(dbHost, dbPort, dbName,
						dbCollection);
				table.drop();
				table.close();
			}
		}

		@Override
		public void preCreateTable(Table tbl) throws MetaException {
			// nothing to do...
		}

		@Override
		public void preDropTable(Table tbl) throws MetaException {
			// nothing to do...
		}

		@Override
		public void rollbackCreateTable(Table tbl) throws MetaException {
			// nothing to do...
		}

		@Override
		public void rollbackDropTable(Table tbl) throws MetaException {
			// nothing to do...
		}

	}
}
