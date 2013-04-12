package org.yong3.hive.mongo;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde.Constants;
import org.apache.hadoop.hive.serde2.SerDe;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.AbstractPrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.log4j.Logger;

public class MongoSerDe implements SerDe {
	static final String HIVE_TYPE_DOUBLE = "double";
	static final String HIVE_TYPE_FLOAT = "float";
	static final String HIVE_TYPE_BOOLEAN = "boolean";
	static final String HIVE_TYPE_BIGINT = "bigint";
	static final String HIVE_TYPE_TINYINT = "tinyint";
	static final String HIVE_TYPE_SMALLINT = "smallint";
	static final String HIVE_TYPE_INT = "int";

	private final MapWritable cachedWritable = new MapWritable();

	private int fieldCount;
	private StructObjectInspector objectInspector;
	private List<String> columnNames;
	String[] columnTypesArray;
	private List<Object> row;
	
	Logger log = Logger.getLogger(MongoSerDe.class);

	@Override
	public void initialize(final Configuration conf, final Properties tbl)
			throws SerDeException {
		final String columnString = tbl
				.getProperty(ConfigurationUtil.COLUMN_MAPPING);
		if (StringUtils.isBlank(columnString)) {
			throw new SerDeException("No column mapping found, use "
					+ ConfigurationUtil.COLUMN_MAPPING);
		}
		final String[] columnNamesArray = ConfigurationUtil
				.getAllColumns(columnString);
		fieldCount = columnNamesArray.length;
		columnNames = new ArrayList<String>(columnNamesArray.length);
		columnNames.addAll(Arrays.asList(columnNamesArray));
		
		log.debug("column names in mongo collection: " + columnNames);
		
		String hiveColumnNameProperty = tbl.getProperty(Constants.LIST_COLUMNS);
		List<String> hiveColumnNameArray = new ArrayList<String>();
		
		if (hiveColumnNameProperty != null && hiveColumnNameProperty.length() > 0) {
			hiveColumnNameArray = Arrays.asList(hiveColumnNameProperty.split("[,:;]"));
		}
		log.debug("column names in hive table: " + hiveColumnNameArray);
		
		String columnTypeProperty = tbl
				.getProperty(Constants.LIST_COLUMN_TYPES);
		// System.err.println("column types:" + columnTypeProperty);
		columnTypesArray = columnTypeProperty.split("[,:;]");
		log.debug("column types in hive table: " + columnTypesArray);

		final List<ObjectInspector> fieldOIs = new ArrayList<ObjectInspector>(
				columnNamesArray.length);
		for (int i = 0; i < columnNamesArray.length; i++) {
			if (HIVE_TYPE_INT.equalsIgnoreCase(columnTypesArray[i])) {
				fieldOIs
						.add(PrimitiveObjectInspectorFactory.javaIntObjectInspector);
			} else if (MongoSerDe.HIVE_TYPE_SMALLINT.equalsIgnoreCase(columnTypesArray[i])) {
				fieldOIs
						.add(PrimitiveObjectInspectorFactory.javaShortObjectInspector);
			} else if (MongoSerDe.HIVE_TYPE_TINYINT.equalsIgnoreCase(columnTypesArray[i])) {
				fieldOIs
						.add(PrimitiveObjectInspectorFactory.javaByteObjectInspector);
			} else if (MongoSerDe.HIVE_TYPE_BIGINT.equalsIgnoreCase(columnTypesArray[i])) {
				fieldOIs
						.add(PrimitiveObjectInspectorFactory.javaLongObjectInspector);
			} else if (MongoSerDe.HIVE_TYPE_BOOLEAN.equalsIgnoreCase(columnTypesArray[i])) {
				fieldOIs
						.add(PrimitiveObjectInspectorFactory.javaBooleanObjectInspector);
			} else if (MongoSerDe.HIVE_TYPE_FLOAT.equalsIgnoreCase(columnTypesArray[i])) {
				fieldOIs
						.add(PrimitiveObjectInspectorFactory.javaFloatObjectInspector);
			} else if (MongoSerDe.HIVE_TYPE_DOUBLE.equalsIgnoreCase(columnTypesArray[i])) {
				fieldOIs
						.add(PrimitiveObjectInspectorFactory.javaDoubleObjectInspector);
			} else {
				// treat as string
				fieldOIs
						.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
			}
		}
		objectInspector = ObjectInspectorFactory
				.getStandardStructObjectInspector(hiveColumnNameArray, fieldOIs);
		row = new ArrayList<Object>(columnNamesArray.length);
	}

	@Override
	public Object deserialize(Writable wr) throws SerDeException {
		if (!(wr instanceof MapWritable)) {
			throw new SerDeException("Expected MapWritable, received "
					+ wr.getClass().getName());
		}

		final MapWritable input = (MapWritable) wr;
		final Text t = new Text();
		row.clear();

		for (int i = 0; i < fieldCount; i++) {
			t.set(columnNames.get(i));
			final Writable value = input.get(t);
			if (value != null && !NullWritable.get().equals(value)) {
				//parse as double to avoid NumberFormatException...
				//TODO:need more test,especially for type 'bigint'
				if (HIVE_TYPE_INT.equalsIgnoreCase(columnTypesArray[i])) {
					row.add(Double.valueOf(value.toString()).intValue());
				} else if (MongoSerDe.HIVE_TYPE_SMALLINT.equalsIgnoreCase(columnTypesArray[i])) {
					row.add(Double.valueOf(value.toString()).shortValue());
				} else if (MongoSerDe.HIVE_TYPE_TINYINT.equalsIgnoreCase(columnTypesArray[i])) {
					row.add(Double.valueOf(value.toString()).byteValue());
				} else if (MongoSerDe.HIVE_TYPE_BIGINT.equalsIgnoreCase(columnTypesArray[i])) {
					row.add(Long.valueOf(value.toString()));
				} else if (MongoSerDe.HIVE_TYPE_BOOLEAN.equalsIgnoreCase(columnTypesArray[i])) {
					row.add(Boolean.valueOf(value.toString()));
				} else if (MongoSerDe.HIVE_TYPE_FLOAT.equalsIgnoreCase(columnTypesArray[i])) {
					row.add(Double.valueOf(value.toString()).floatValue());
				} else if (MongoSerDe.HIVE_TYPE_DOUBLE.equalsIgnoreCase(columnTypesArray[i])) {
					row.add(Double.valueOf(value.toString()));
				} else {
					row.add(value.toString());
				}
			} else {
				row.add(null);
			}
		}

		return row;
	}

	@Override
	public ObjectInspector getObjectInspector() throws SerDeException {
		return objectInspector;
	}

	@Override
	public Class<? extends Writable> getSerializedClass() {
		return MapWritable.class;
	}

	@Override
	public Writable serialize(final Object obj, final ObjectInspector inspector)
			throws SerDeException {
		final StructObjectInspector structInspector = (StructObjectInspector) inspector;
		final List<? extends StructField> fields = structInspector
				.getAllStructFieldRefs();
		if (fields.size() != columnNames.size()) {
			throw new SerDeException(String.format(
					"Required %d columns, received %d.", columnNames.size(),
					fields.size()));
		}
		
		cachedWritable.clear();
		for (int c = 0; c < fieldCount; c++) {
			StructField structField = fields.get(c);
			if (structField != null) {
				final Object field = structInspector.getStructFieldData(obj,
						fields.get(c));
				
				//TODO:currently only support hive primitive type
				final AbstractPrimitiveObjectInspector fieldOI = (AbstractPrimitiveObjectInspector)fields.get(c)
						.getFieldObjectInspector();

				Writable value = (Writable)fieldOI.getPrimitiveWritableObject(field);
				
				if (value == null) {
					if(PrimitiveCategory.STRING.equals(fieldOI.getPrimitiveCategory())){
						value = NullWritable.get();	
						//value = new Text("");
					}else{
						//TODO: now all treat as number
						value = new IntWritable(0);
					}
				}
				cachedWritable.put(new Text(columnNames.get(c)), value);
			}
		}
		return cachedWritable;
	}

	/*
	@Override
	public SerDeStats getSerDeStats() {
		// TODO Auto-generated method stub
		return null;
	}*/

}
