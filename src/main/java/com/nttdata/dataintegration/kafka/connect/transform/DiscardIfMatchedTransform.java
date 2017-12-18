/*
* TestField.java Created on 2017年12月14日
* Copyright 2017@NTT DATA
* All right reserved. 
*/
package com.nttdata.dataintegration.kafka.connect.transform;

import java.util.Map;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.transforms.Transformation;
import org.apache.kafka.connect.transforms.util.SimpleConfig;

public class DiscardIfMatchedTransform <R extends ConnectRecord<R>> implements Transformation<R>{

	public static final String OVERVIEW_DOC = "This is a test to add value a plus of customized str such as 'abc'.";

    public static final String FIELD_CONFIG = "discard.field";
    public static final String MATCHED_VAL_CONFIG = "discard.value";

    public static final ConfigDef CONFIG_DEF = new ConfigDef()
            .define(FIELD_CONFIG, ConfigDef.Type.STRING, ConfigDef.NO_DEFAULT_VALUE, ConfigDef.Importance.HIGH,
                    "Field names on the record value to add a plus of 'abc'.")
    		.define(MATCHED_VAL_CONFIG, ConfigDef.Type.STRING, ConfigDef.NO_DEFAULT_VALUE, ConfigDef.Importance.HIGH,
            "Field names on the record value to add a plus of 'abc'.");

    private String fieldName;
    private String condVal;

    @Override
    public void configure(Map<String, ?> configs) {
        final SimpleConfig config = new SimpleConfig(CONFIG_DEF, configs);
        fieldName = config.getString(FIELD_CONFIG);
        this.condVal = config.getString(MATCHED_VAL_CONFIG);
    }

    @Override
    public R apply(R record) {
    	final Struct valueStruct = (Struct) record.value();
    	final Field field = record.valueSchema().field(fieldName);
    	if (field == null) return record;
    	
    	Object value = valueStruct.get(field);
    	String valStr = String.valueOf(value);
    	if (valStr == null || valStr.trim().equals("")) return record;
    	
    	if (valStr.equalsIgnoreCase(condVal)) {
    		System.out.print("Matched :" + condVal + ", this record would to be discarded.");
    		return null;
    	} 
    	return record;
    }

    @Override
    public ConfigDef config() {
        return CONFIG_DEF;
    }

    @Override
    public void close() {
    	
    }

}
