package org.yong3.hive.mongo;

import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.apache.hadoop.mapred.JobConf;

import com.google.common.collect.ImmutableSet;

@SuppressWarnings("deprecation")
public class ConfigurationUtil {
	static final String DB_NAME = "mongo.db";
	static final String COLLECTION_NAME = "mongo.collection";
	static final String DB_HOST = "mongo.host";
	static final String DB_PORT = "mongo.port";
	static final String COLUMN_MAPPING = "mongo.column.mapping";
	
	public static final Set<String> ALL_PROPERTIES = ImmutableSet.of(
			DB_NAME,
			COLLECTION_NAME,
			DB_HOST,
			DB_PORT,
			COLUMN_MAPPING);

	public final static String getDBName(JobConf conf){
		return conf.get(DB_NAME);
	}
	
	public final static String getCollectionName(JobConf conf){
		return conf.get(COLLECTION_NAME);
	}
	
	public final static String getDBHost(JobConf conf){
		return conf.get(DB_HOST);
	}
	
	public final static String getDBPort(JobConf conf){
		return conf.get(DB_PORT);
	}
	
	public final static String getColumnMapping(JobConf conf){
		return conf.get(COLUMN_MAPPING);
	}
	
	  public static void copyGDataProperties(Properties from, Map<String, String> to) {
		    for (String key : ALL_PROPERTIES) {
		      String value = from.getProperty(key);
		      if (value != null) {
		        to.put(key, value);
		      }
		    }
		  }
	  
	  public static String[] getAllColumns(String columnMappingString){
		  return columnMappingString.split(",");
	  }
}
