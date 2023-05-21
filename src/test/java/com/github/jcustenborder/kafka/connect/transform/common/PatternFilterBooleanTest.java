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
public class PatternFilterBooleanTest {
  public PatternFilter.Value transform;

  @SuppressWarnings("unchecked")
  @BeforeEach
  public void before() {
    this.transform = new PatternFilter.Value();
    this.transform.configure(
        ImmutableMap.of(
            PatternFilterConfig.FIELD_CONFIG, "input",
            PatternFilterConfig.PATTERN_CONFIG, "true"
        )
    );
  }
  
  SinkRecord structBoolean(boolean value) {
    Schema schema = SchemaBuilder.struct()
        .field("input", Schema.BOOLEAN_SCHEMA)
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
    assertNull(this.transform.apply(structBoolean(true)));
  }

  @SuppressWarnings("unchecked")
  @Test
  public void notFiltered() {
    assertNotNull(this.transform.apply(structBoolean(false)));
  }

}
