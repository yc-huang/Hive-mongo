package org.yong3.hive.mongo;

import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;

import com.google.common.collect.ImmutableSet;

public class ConfigurationUtil {
	public static final String DB_NAME = "mongo.db";
	public static final String COLLECTION_NAME = "mongo.collection";
	public static final String DB_HOST = "mongo.host";
	public static final String DB_PORT = "mongo.port";
	public static final String COLUMN_MAPPING = "mongo.column.mapping";

	public static final Set<String> ALL_PROPERTIES = ImmutableSet.of(DB_NAME,
			COLLECTION_NAME, DB_HOST, DB_PORT, COLUMN_MAPPING);

	public final static String getDBName(Configuration conf) {
		return conf.get(DB_NAME);
	}

	public final static String getCollectionName(Configuration conf) {
		return conf.get(COLLECTION_NAME);
	}

	public final static String getDBHost(Configuration conf) {
		return conf.get(DB_HOST);
	}

	public final static String getDBPort(Configuration conf) {
		return conf.get(DB_PORT);
	}

	public final static String getColumnMapping(Configuration conf) {
		return conf.get(COLUMN_MAPPING);
	}

	public static void copyMongoProperties(Properties from,
			Map<String, String> to) {
		for (String key : ALL_PROPERTIES) {
			String value = from.getProperty(key);
			if (value != null) {
				to.put(key, value);
			}
		}
	}

	public static String[] getAllColumns(String columnMappingString) {
		return columnMappingString.split(",");
	}
}
