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

public class PlusValueField <R extends ConnectRecord<R>> implements Transformation<R>{

	public static final String OVERVIEW_DOC = "This is a test to add value a plus of customized str such as 'abc'.";

    public static final String FIELD_CONFIG = "field";

    public static final ConfigDef CONFIG_DEF = new ConfigDef()
            .define(FIELD_CONFIG, ConfigDef.Type.STRING, ConfigDef.NO_DEFAULT_VALUE, ConfigDef.Importance.HIGH,
                    "Field names on the record value to add a plus of 'abc'.");

    private String fieldName;

    @Override
    public void configure(Map<String, ?> configs) {
        final SimpleConfig config = new SimpleConfig(CONFIG_DEF, configs);
        fieldName = config.getString(FIELD_CONFIG);
    }

    @Override
    public R apply(R record) {
    	final Struct valueStruct = (Struct) record.value();
    	final Field field = record.valueSchema().field(fieldName);
    	if (field == null) return record;
    	
    	Object value = valueStruct.get(field);
    	value = (String)value + "abc";
    	valueStruct.put(field, value);
        return record.newRecord(record.topic(), record.kafkaPartition(), record.keySchema(), record.key(), record.valueSchema(), valueStruct, record.timestamp());
    }

    @Override
    public ConfigDef config() {
        return CONFIG_DEF;
    }

    @Override
    public void close() {
    	
    }

}
