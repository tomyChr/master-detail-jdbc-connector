package io.confluent.connect.jdbc.source;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTaskContext;
import org.apache.kafka.connect.storage.OffsetStorageReader;
import org.easymock.EasyMock;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Timestamp;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.easymock.EasyMock.anyObject;
import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.replay;
import static org.junit.Assert.*;

// @RunWith(PowerMockRunner.class)
public class JdbcMasterDetailSourceTaskTest extends JdbcSourceTaskTestBase {

    private static final Logger log = LoggerFactory.getLogger(JdbcMasterDetailSourceTaskTest.class);
    final static String TIME_STAMP_COLUMN = "tst";
    final static String INCREMENT_COLUMN = "inc";

    @Test
    public void testSingleMasterDetailBulkTable() throws Exception {
        db.createTable(SINGLE_TABLE_NAME, "id", "INT", "name", "VARCHAR(10)", "value", "INT");

        long startTime = time.milliseconds();
        task.start(masterDetailTableConfig(false, JdbcSourceConnectorConfig.MODE_BULK, null, null));

        db.insert(SINGLE_TABLE_NAME, "id", 1, "name", "group1", "value", 1);
        db.insert(SINGLE_TABLE_NAME, "id", 1, "name", "group1", "value", 2);
        db.insert(SINGLE_TABLE_NAME, "id", 1, "name", "group1", "value", 3);
        db.insert(SINGLE_TABLE_NAME, "id", 2, "name", "group2", "value", 1);
        db.insert(SINGLE_TABLE_NAME, "id", 2, "name", "group2", "value", 2);
        db.insert(SINGLE_TABLE_NAME, "id", 3, "name", "group3", "value", 1);

        // Both tables should be polled immediately, in order
        List<SourceRecord> records = task.poll();

        assertEquals(startTime, time.milliseconds());
        assertEquals(3, records.size());
        assertEquals(SINGLE_TABLE_PARTITION, records.get(0).sourcePartition());

        validateResultTable(records, 3, SINGLE_TABLE_NAME);
        validateResultRecord(records, 0, 1, 3, "group1", +1);
        validateResultRecord(records, 1, 2, 2, "group2", +1);
        validateResultRecord(records, 2, 3, 1, "group3", +1);
    }

    @Test
    public void testSingleMasterDetailTimestampTable() throws Exception {

        db.createTable(SINGLE_TABLE_NAME, "id", "INT", "name", "VARCHAR(10)", "value", "INT", TIME_STAMP_COLUMN, "TIMESTAMP NOT NULL");

        OffsetStorageReader reader = createMock(OffsetStorageReader.class);
        EasyMock.expect(reader.offsets(anyObject())).andReturn(null);

        SourceTaskContext taskContext = createMock(SourceTaskContext.class);
        EasyMock.expect(taskContext.offsetStorageReader()).andReturn(reader).anyTimes();

        replay(taskContext, reader);

        task.initialize(taskContext);

        long startTime = time.milliseconds();
        task.start(masterDetailTableConfig(false, JdbcSourceConnectorConfig.MODE_TIMESTAMP, TIME_STAMP_COLUMN, null));

        String tst = new Timestamp(System.currentTimeMillis()).toString();
        db.insert(SINGLE_TABLE_NAME, "id", 1, "name", "group1", "value", 1, TIME_STAMP_COLUMN, tst);
        db.insert(SINGLE_TABLE_NAME, "id", 1, "name", "group1", "value", 2, TIME_STAMP_COLUMN, tst);
        db.insert(SINGLE_TABLE_NAME, "id", 1, "name", "group1", "value", 3, TIME_STAMP_COLUMN, tst);
        db.insert(SINGLE_TABLE_NAME, "id", 2, "name", "group2", "value", 1, TIME_STAMP_COLUMN, tst);
        db.insert(SINGLE_TABLE_NAME, "id", 2, "name", "group2", "value", 2, TIME_STAMP_COLUMN, tst);
        db.insert(SINGLE_TABLE_NAME, "id", 3, "name", "group3", "value", 1, TIME_STAMP_COLUMN, tst);

        // Both tables should be polled immediately, in order
        List<SourceRecord> records = task.poll();

        assertEquals(startTime, time.milliseconds());
        assertEquals(3, records.size());
        Map<String, String> partitionForV1 = new HashMap<>();
        partitionForV1.put(JdbcSourceConnectorConstants.TABLE_NAME_KEY, SINGLE_TABLE_NAME);
        partitionForV1.put(
                JdbcSourceConnectorConstants.OFFSET_PROTOCOL_VERSION_KEY,
                JdbcSourceConnectorConstants.PROTOCOL_VERSION_ONE
        );
        assertEquals(partitionForV1, records.get(0).sourcePartition());

        validateResultTable(records, 3, SINGLE_TABLE_NAME);
        validateResultRecord(records, 0, 3, 1,"group3", -1);
        validateResultRecord(records, 1, 2, 2,"group2", -1);
        validateResultRecord(records, 2, 1, 3,"group1", -1);
    }

    @Test
    public void testSingleMasterDetailIncrementTable() throws Exception {

        db.createTable(SINGLE_TABLE_NAME, "id", "INT", "name", "VARCHAR(10)", "value", "INT", INCREMENT_COLUMN, "INT NOT NULL");

        OffsetStorageReader reader = createMock(OffsetStorageReader.class);
        EasyMock.expect(reader.offsets(anyObject())).andReturn(null);

        SourceTaskContext taskContext = createMock(SourceTaskContext.class);
        EasyMock.expect(taskContext.offsetStorageReader()).andReturn(reader).anyTimes();

        replay(taskContext, reader);

        task.initialize(taskContext);

        long startTime = time.milliseconds();
        task.start(masterDetailTableConfig(false, JdbcSourceConnectorConfig.MODE_INCREMENTING, null, INCREMENT_COLUMN));

        int inc = 101;
        db.insert(SINGLE_TABLE_NAME, "id", 1, "name", "group1", "value", 1, INCREMENT_COLUMN, inc++);
        db.insert(SINGLE_TABLE_NAME, "id", 1, "name", "group1", "value", 2, INCREMENT_COLUMN, inc++);
        db.insert(SINGLE_TABLE_NAME, "id", 1, "name", "group1", "value", 3, INCREMENT_COLUMN, inc++);
        db.insert(SINGLE_TABLE_NAME, "id", 2, "name", "group2", "value", 1, INCREMENT_COLUMN, inc++);
        db.insert(SINGLE_TABLE_NAME, "id", 2, "name", "group2", "value", 2, INCREMENT_COLUMN, inc++);
        db.insert(SINGLE_TABLE_NAME, "id", 3, "name", "group3", "value", 1, INCREMENT_COLUMN, inc++);

        // Both tables should be polled immediately, in order
        List<SourceRecord> records = task.poll();

        assertEquals(startTime, time.milliseconds());
        assertEquals(3, records.size());
        Map<String, String> partitionForV1 = new HashMap<>();
        partitionForV1.put(JdbcSourceConnectorConstants.TABLE_NAME_KEY, SINGLE_TABLE_NAME);
        partitionForV1.put(
                JdbcSourceConnectorConstants.OFFSET_PROTOCOL_VERSION_KEY,
                JdbcSourceConnectorConstants.PROTOCOL_VERSION_ONE
        );
        assertEquals(partitionForV1, records.get(0).sourcePartition());

        validateResultTable(records, 3, SINGLE_TABLE_NAME);
        validateResultRecord(records, 0, 1, 3, "group1", +1);
        validateResultRecord(records, 1, 2, 2, "group2", +1);
        validateResultRecord(records, 2, 3, 1, "group3", +1);
    }

    private static void validateResultTable(List<SourceRecord> records, int expected, String table) {
        assertEquals(expected, records.size());
        for (SourceRecord record : records) {
            assertEquals(table, record.sourcePartition().get(JdbcSourceConnectorConstants.TABLE_NAME_KEY));
            log.debug("ResultTable record {}", record.value());
        }
    }

    private static void validateResultRecord(List<SourceRecord> records, int index, int expectedId, int expectedDetails, String expectedName, int valueAdd) {
        assertTrue("Should contain expected result", index < records.size());
        SourceRecord record = records.get(index);
        assertNotNull("Record " + index + " must not be null", record);
        Struct struct = (Struct)record.value();
        assertEquals("Record " + index + " should contain " + expectedDetails + " detail records", expectedDetails, struct.getArray("values").size());
        assertEquals("Master field 'id' should contain " + expectedId, expectedId, struct.get("id"));
        assertEquals("Master field 'name' should contain " + expectedName, expectedName, struct.get("name"));
        List<Struct> details = struct.getArray("values");
        int value = valueAdd > 0 ? 0 : expectedDetails+1;
        for (Struct detail: details) {
            value += valueAdd;
            assertEquals("Detail field 'id' should contain " + expectedId, expectedId, detail.get("id"));
            assertEquals("Detail field 'name' should contain " + expectedName, expectedName, detail.get("name"));
            assertEquals("Detail field 'value' should contain " + value , value, detail.get("value"));
        }
    }

    protected Map<String, String> masterDetailTableConfig(boolean completeMapping, String mode, String timeStampColumn, String incrementingColumn) {
        Map<String, String> props = new HashMap<>();
        props.put(JdbcSourceConnectorConfig.CONNECTION_URL_CONFIG, db.getUrl());
        props.put(JdbcSourceTaskConfig.TABLES_CONFIG, SINGLE_TABLE_NAME);
        props.put(JdbcSourceConnectorConfig.MODE_CONFIG, mode);
        if (mode == JdbcSourceConnectorConfig.MODE_TIMESTAMP) {
            props.put(JdbcSourceTaskConfig.TIMESTAMP_COLUMN_NAME_CONFIG, timeStampColumn);
        } else if (mode == JdbcSourceConnectorConfig.MODE_INCREMENTING) {
            props.put(JdbcSourceTaskConfig.INCREMENTING_COLUMN_NAME_CONFIG, incrementingColumn);
        }
        props.put(JdbcSourceTaskConfig.TOPIC_PREFIX_CONFIG, TOPIC_PREFIX);
        if (completeMapping) {
            props.put(JdbcSourceTaskConfig.NUMERIC_MAPPING_CONFIG, JdbcSourceConnectorConfig.NumericMapping.BEST_FIT.toString());
        } else {
            props.put(JdbcSourceTaskConfig.NUMERIC_PRECISION_MAPPING_CONFIG, "true");
        }
        props.put(JdbcSourceConnectorConfig.MASTERDETAIL_GROUPING_COLUMNS_CONFIG, "name");
        props.put(JdbcSourceConnectorConfig.MASTERDETAIL_MASTER_COLUMNS_CONFIG, "id, name");
        props.put(JdbcSourceConnectorConfig.MASTERDETAIL_DETAIL_COLUMNS_CONFIG, "id, name, value");
        props.put(JdbcSourceConnectorConfig.MASTERDETAIL_DETAIL_NAME_CONFIG, "values");
        return props;
    }


}
