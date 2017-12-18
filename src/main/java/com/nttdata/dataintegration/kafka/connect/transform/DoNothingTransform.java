/*
* TestField.java Created on 2017年12月14日
* Copyright 2017@NTT DATA
* All right reserved. 
*/
package com.nttdata.dataintegration.kafka.connect.transform;

import java.util.List;
import java.util.Map;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.transforms.Transformation;

public class DoNothingTransform <R extends ConnectRecord<R>> implements Transformation<R>{

	public static final String OVERVIEW_DOC = "This is a transformer which just prints valueSchma without any modification.";

    public static final ConfigDef CONFIG_DEF = new ConfigDef();


    @Override
    public void configure(Map<String, ?> configs) {
    }

    @Override
    public R apply(R record) {
    	final Struct valueStruct = (Struct) record.value();
    	if (valueStruct == null) return record;
    	
    	List<Field> kvs = record.valueSchema().fields();
    	StringBuffer buf = new StringBuffer();
    	for(Field anyField : kvs){
    		buf.append(anyField.name()).append("==").append(valueStruct.get(anyField)).append(",");
    	}
    	System.out.println(buf.toString());
    	
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
