package org.yong3.hive.mongo;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.Map.Entry;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde.Constants;
import org.apache.hadoop.hive.serde2.SerDe;
import org.apache.hadoop.hive.serde2.SerDeException;
//import org.apache.hadoop.hive.serde2.SerDeStats;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.AbstractPrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

public class MongoSerDe implements SerDe {
	private final MapWritable cachedWritable = new MapWritable();

	private int fieldCount;
	private StructObjectInspector objectInspector;
	private List<String> columnNames;
	String[] columnTypesArray;
	private List<Object> row;

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

		String columnTypeProperty = tbl
				.getProperty(Constants.LIST_COLUMN_TYPES);
		// System.err.println("column types:" + columnTypeProperty);
		columnTypesArray = columnTypeProperty.split(":");

		final List<ObjectInspector> fieldOIs = new ArrayList<ObjectInspector>(
				columnNamesArray.length);
		for (int i = 0; i < columnNamesArray.length; i++) {
			if ("int".equalsIgnoreCase(columnTypesArray[i])) {
				fieldOIs
						.add(PrimitiveObjectInspectorFactory.javaIntObjectInspector);
			} else if ("smallint".equalsIgnoreCase(columnTypesArray[i])) {
				fieldOIs
						.add(PrimitiveObjectInspectorFactory.javaShortObjectInspector);
			} else if ("tinyint".equalsIgnoreCase(columnTypesArray[i])) {
				fieldOIs
						.add(PrimitiveObjectInspectorFactory.javaByteObjectInspector);
			} else if ("bigint".equalsIgnoreCase(columnTypesArray[i])) {
				fieldOIs
						.add(PrimitiveObjectInspectorFactory.javaLongObjectInspector);
			} else if ("boolean".equalsIgnoreCase(columnTypesArray[i])) {
				fieldOIs
						.add(PrimitiveObjectInspectorFactory.javaBooleanObjectInspector);
			} else if ("float".equalsIgnoreCase(columnTypesArray[i])) {
				fieldOIs
						.add(PrimitiveObjectInspectorFactory.javaFloatObjectInspector);
			} else if ("double".equalsIgnoreCase(columnTypesArray[i])) {
				fieldOIs
						.add(PrimitiveObjectInspectorFactory.javaDoubleObjectInspector);
			} else {
				// treat as string
				fieldOIs
						.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
			}
		}
		objectInspector = ObjectInspectorFactory
				.getStandardStructObjectInspector(columnNames, fieldOIs);
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
				if ("int".equalsIgnoreCase(columnTypesArray[i])) {
					row.add(Integer.valueOf(value.toString()));
				} else if ("smallint".equalsIgnoreCase(columnTypesArray[i])) {
					row.add(Short.valueOf(value.toString()));
				} else if ("tinyint".equalsIgnoreCase(columnTypesArray[i])) {
					row.add(Byte.valueOf(value.toString()));
				} else if ("bigint".equalsIgnoreCase(columnTypesArray[i])) {
					row.add(Long.valueOf(value.toString()));
				} else if ("boolean".equalsIgnoreCase(columnTypesArray[i])) {
					row.add(Boolean.valueOf(value.toString()));
				} else if ("float".equalsIgnoreCase(columnTypesArray[i])) {
					row.add(Float.valueOf(value.toString()));
				} else if ("double".equalsIgnoreCase(columnTypesArray[i])) {
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
				
				final ObjectInspector fieldOI = fields.get(c)
						.getFieldObjectInspector();
				//TODO:currently only support hive primitive type
				final AbstractPrimitiveObjectInspector fieldStringOI = (AbstractPrimitiveObjectInspector) fieldOI;
				Writable value = (Writable)fieldStringOI.getPrimitiveWritableObject(field);
				
				if (value == null) {
					if(PrimitiveCategory.STRING.equals(fieldStringOI.getPrimitiveCategory())){
						//value = NullWritable.get();	
						value = new Text("");
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
