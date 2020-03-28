/*
 * Copyright (C) 2019 Can Elmas <canelm@gmail.com>
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.canelmas.kafka.connect;

import io.confluent.connect.storage.errors.PartitionException;
import io.confluent.connect.storage.partitioner.TimeBasedPartitioner;
import io.confluent.connect.storage.partitioner.TimestampExtractor;
import io.confluent.connect.storage.util.DataUtils;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Locale;
import java.util.Map;

public final class FieldAndTimeBasedPartitioner<T> extends TimeBasedPartitioner<T> {

    private static final Logger log = LoggerFactory.getLogger(FieldAndTimeBasedPartitioner.class);

    private long partitionDurationMs;
    private DateTimeFormatter formatter;
    private TimestampExtractor timestampExtractor;

    private PartitionFieldExtractor partitionFieldExtractor;

    protected void init(long partitionDurationMs, String pathFormat, Locale locale, DateTimeZone timeZone, Map<String, Object> config) {

        this.delim = (String)config.get("directory.delim");
        this.partitionDurationMs = partitionDurationMs;

        try {

            this.formatter = getDateTimeFormatter(pathFormat, timeZone).withLocale(locale);
            this.timestampExtractor = this.newTimestampExtractor((String)config.get("timestamp.extractor"));
            this.timestampExtractor.configure(config);

            this.partitionFieldExtractor = new PartitionFieldExtractor((String)config.get("partition.field"));

        } catch (IllegalArgumentException e) {

            ConfigException ce = new ConfigException("path.format", pathFormat, e.getMessage());
            ce.initCause(e);
            throw ce;
            
        }
    }

    private static DateTimeFormatter getDateTimeFormatter(String str, DateTimeZone timeZone) {
        return DateTimeFormat.forPattern(str).withZone(timeZone);
    }

    public static long getPartition(long timeGranularityMs, long timestamp, DateTimeZone timeZone) {

        long adjustedTimestamp = timeZone.convertUTCToLocal(timestamp);
        long partitionedTime = adjustedTimestamp / timeGranularityMs * timeGranularityMs;

        return timeZone.convertLocalToUTC(partitionedTime, false);
        
    }
    
    public String encodePartition(SinkRecord sinkRecord, long nowInMillis) {

        final Long timestamp = this.timestampExtractor.extract(sinkRecord, nowInMillis);
        final String partitionField = this.partitionFieldExtractor.extract(sinkRecord);

        return this.encodedPartitionForFieldAndTime(sinkRecord, timestamp, partitionField);

    }

    public String encodePartition(SinkRecord sinkRecord) {

        final Long timestamp = this.timestampExtractor.extract(sinkRecord);
        final String partitionFieldValue = this.partitionFieldExtractor.extract(sinkRecord);

        return encodedPartitionForFieldAndTime(sinkRecord, timestamp, partitionFieldValue);

    }

    private String encodedPartitionForFieldAndTime(SinkRecord sinkRecord, Long timestamp, String partitionField) {

        if (timestamp == null) {

            String msg = "Unable to determine timestamp using timestamp.extractor " + this.timestampExtractor.getClass().getName() + " for record: " + sinkRecord;
            log.error(msg);
            throw new ConnectException(msg);

        } else if (partitionField == null) {

            String msg = "Unable to determine partition field using partition.field '" + partitionField  + "' for record: " + sinkRecord;
            log.error(msg);
            throw new ConnectException(msg);

        }  else {

            DateTime bucket = new DateTime(getPartition(this.partitionDurationMs, timestamp.longValue(), this.formatter.getZone()));
            return partitionField + this.delim + bucket.toString(this.formatter);
            
        }
    }

    static class PartitionFieldExtractor {

        private final String fieldName;

        PartitionFieldExtractor(String fieldName) {
            this.fieldName = fieldName;
        }

        String extract(ConnectRecord<?> record) {

            Object value = record.value();

            if (value instanceof Struct) {

                final Object field = DataUtils.getNestedFieldValue(value, fieldName);
                final Schema fieldSchema = DataUtils.getNestedField(record.valueSchema(), fieldName).schema();

                FieldAndTimeBasedPartitioner.log.error("Unsupported type '{}' for partition field.", fieldSchema.type().getName());

                return (String) field;

            } else if (value instanceof Map) {

                return (String) DataUtils.getNestedFieldValue(value, fieldName);

            } else {

                FieldAndTimeBasedPartitioner.log.error("Value is not of Struct or Map type.");
                throw new PartitionException("Error encoding partition.");
                
            }
        }
    }

    @Override
    public long getPartitionDurationMs() {
        return partitionDurationMs;
    }

    @Override
    public TimestampExtractor getTimestampExtractor() {
        return timestampExtractor;
    }
}
