package com.github.jcustenborder.kafka.connect.transform.common;

import com.google.common.collect.ImmutableMap;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

@SuppressWarnings("rawtypes")
public class PatternFilterNumericTest {
  public PatternFilter.Value transform;
  public PatternFilter.Value shortTransform;
  public PatternFilter.Value intTransform;
  public PatternFilter.Value longTransform;
  public PatternFilter.Value floatTransform;

  @SuppressWarnings("unchecked")
  @BeforeEach
  public void before() {
    this.transform = new PatternFilter.Value();
    this.transform.configure(
        ImmutableMap.of(
            PatternFilterConfig.FIELD_CONFIG, "input",
            PatternFilterConfig.PATTERN_CONFIG, "^\\d$"
        )
    );
    
    this.shortTransform = new PatternFilter.Value<>();
    this.shortTransform.configure(
        ImmutableMap.of(
            PatternFilterConfig.FIELD_CONFIG, "input",
            PatternFilterConfig.PATTERN_CONFIG, "^\\d{5}$"
        )
    );    
    
    this.intTransform = new PatternFilter.Value<>();
    this.intTransform.configure(
        ImmutableMap.of(
            PatternFilterConfig.FIELD_CONFIG, "input",
            PatternFilterConfig.PATTERN_CONFIG, "^\\d{10}$"
        )
    );     
    
    this.longTransform = new PatternFilter.Value<>();
    this.longTransform.configure(
        ImmutableMap.of(
            PatternFilterConfig.FIELD_CONFIG, "input",
            PatternFilterConfig.PATTERN_CONFIG, "^\\d{19}$"
        )
    );      
    
    this.floatTransform = new PatternFilter.Value<>();
    this.floatTransform.configure(
        ImmutableMap.of(
            PatternFilterConfig.FIELD_CONFIG, "input",
            PatternFilterConfig.PATTERN_CONFIG, "^\\d\\.\\d+$"
        )
    );
  }
  
  SinkRecord structInt8(byte value) {
    Schema schema = SchemaBuilder.struct()
        .field("input", Schema.INT8_SCHEMA)
        .build();
    Struct struct = new Struct(schema)
        .put("input", value);
    return new SinkRecord(
        "asdf",
        1,
        null,
        null,
        schema,
        struct,
        1234L
    );
  }
  
  SinkRecord structInt16(short value) {
    Schema schema = SchemaBuilder.struct()
        .field("input", Schema.INT16_SCHEMA)
        .build();
    Struct struct = new Struct(schema)
        .put("input", value);
    return new SinkRecord(
        "asdf",
        1,
        null,
        null,
        schema,
        struct,
        1234L
    );
  }
  
  SinkRecord structUInt16(int value) {
    Schema schema = SchemaBuilder.struct()
        .field("input", Schema.INT32_SCHEMA)
        .build();
    Struct struct = new Struct(schema)
        .put("input", value);
    return new SinkRecord(
        "asdf",
        1,
        null,
        null,
        schema,
        struct,
        1234L
    );
  }
  
  SinkRecord structInt32(int value) {
    Schema schema = SchemaBuilder.struct()
        .field("input", Schema.INT32_SCHEMA)
        .build();
    Struct struct = new Struct(schema)
        .put("input", value);
    return new SinkRecord(
        "asdf",
        1,
        null,
        null,
        schema,
        struct,
        1234L
    );
  }
  
  SinkRecord structUInt32(long value) {
    Schema schema = SchemaBuilder.struct()
        .field("input", Schema.INT64_SCHEMA)
        .build();
    Struct struct = new Struct(schema)
        .put("input", value);
    return new SinkRecord(
        "asdf",
        1,
        null,
        null,
        schema,
        struct,
        1234L
    );
  }  
  
  SinkRecord structFloat(Float value) {
    Schema schema = SchemaBuilder.struct()
        .field("input", Schema.FLOAT32_SCHEMA)
        .build();
    Struct struct = new Struct(schema)
        .put("input", value);
    return new SinkRecord(
        "asdf",
        1,
        null,
        null,
        schema,
        struct,
        1234L
    );    
  }
  
  @SuppressWarnings("unchecked")
  @Test
  public void filtered() {
    assertNull(this.transform.apply(structInt8((byte)1)));
    assertNull(this.shortTransform.apply(structInt16(Short.MAX_VALUE)));  
    assertNull(this.shortTransform.apply(structUInt16(Short.MAX_VALUE*2)));
    assertNull(this.intTransform.apply(structInt32(Integer.MAX_VALUE)));  
    assertNull(this.longTransform.apply(structUInt32(Long.MAX_VALUE)));
    assertNull(this.floatTransform.apply(structFloat(1.0123F)));
  }

  @SuppressWarnings("unchecked")
  @Test
  public void notFiltered() {
    assertNotNull(this.transform.apply(structInt8((byte)10)));
    assertNotNull(this.shortTransform.apply(structInt16((short)4000)));
    assertNotNull(this.intTransform.apply(structInt32(-132093102)));  
    assertNotNull(this.longTransform.apply(structUInt32(Long.MAX_VALUE*2)));
    assertNotNull(this.floatTransform.apply(structFloat(-10.0123F)));
  }

}
