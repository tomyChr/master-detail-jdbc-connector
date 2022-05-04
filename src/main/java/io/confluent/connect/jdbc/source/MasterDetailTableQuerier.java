/*
 * Copyright 2018 Confluent Inc.
 */

package io.confluent.connect.jdbc.source;


import io.confluent.connect.jdbc.dialect.DatabaseDialect;
import io.confluent.connect.jdbc.util.ResultSetMetaDataFilter;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * The master/detail table querier is a wrapper around Jdbc table queriers based
 * {@code TableQuerier}.
 */
public class MasterDetailTableQuerier extends TableQuerier {

  private enum NextState {
    NEXT, // next() will be delegated to wrapped TableQuerier
    SKIP, // next() will not change the current ResultSet and return True
    END   // ResultSet is at the end - next() will return False
  }

  private static final Logger log = LoggerFactory.getLogger(MasterDetailTableQuerier.class);

  protected TableQuerier wrappedTableQuerier;
  final List<String> masterColumns;
  final String detailName;
  final List<String> detailColumns;
  final List<String> groupingColumns;

  protected SchemaMapping masterSchemaMapping;
  protected SchemaMapping detailSchemaMapping;
  protected SchemaMapping groupingSchemaMapping;

  protected NextState skipNext = NextState.NEXT;
  protected SourceRecord nextRecord = null;

  public MasterDetailTableQuerier(TableQuerier tableQuerier, JdbcSourceTaskConfig config) {
    super(tableQuerier.dialect,
            tableQuerier.mode,
            tableQuerier.mode == QueryMode.TABLE ? tableQuerier.tableId.tableName() :
                    tableQuerier.query,
            tableQuerier.topicPrefix,
            tableQuerier.suffix);
    this.wrappedTableQuerier = tableQuerier;

    masterColumns = config.getList(JdbcSourceTaskConfig.MASTERDETAIL_MASTER_COLUMNS_CONFIG);
    detailName = config.getString(JdbcSourceTaskConfig.MASTERDETAIL_DETAIL_NAME_CONFIG);
    detailColumns = config.getList(JdbcSourceTaskConfig.MASTERDETAIL_DETAIL_COLUMNS_CONFIG);
    groupingColumns = config.getList(JdbcSourceTaskConfig.MASTERDETAIL_GROUPING_COLUMNS_CONFIG);
  }

  @Override
  public void maybeStartQuery(Connection db) throws SQLException {
    log.debug("MasterDetailTableQuerier: mayStartQuery - setup schema mappings, etc.");
    wrappedTableQuerier.maybeStartQuery(db);
    // be aware that the detail schema is required for building the master schema
    detailSchemaMapping = createSchemaMapping(wrappedTableQuerier.resultSet, detailColumns,
            dialect, null, null);
    if (masterColumns.size() > 0) {
      masterSchemaMapping = createSchemaMapping(wrappedTableQuerier.resultSet, masterColumns,
              dialect, detailName, detailSchemaMapping);
    }
    groupingSchemaMapping = createSchemaMapping(wrappedTableQuerier.resultSet, groupingColumns,
            dialect, null, null);
  }

  protected SchemaMapping createSchemaMapping(ResultSet resultSet,
                                              final List<String> columns,
                                              final DatabaseDialect dialect,
                                              String detailName,
                                              SchemaMapping detailSchemaMapping) throws SQLException {
    String schemaName = tableId != null ? tableId.tableName() : null; // backwards compatible
    return SchemaMapping.create(schemaName,
            new ResultSetMetaDataFilter(resultSet.getMetaData(), columns),
            dialect,
            detailName,
            detailSchemaMapping);
  }

  @Override
  public void reset(long now, boolean resetOffset) {
    log.debug("MasterDetailTableQuerier: reset");
    wrappedTableQuerier.reset(now, resetOffset);
    masterSchemaMapping = null;
    detailSchemaMapping = null;
    groupingSchemaMapping = null;
    skipNext = NextState.NEXT;
    nextRecord = null;
  }

  @Override
  protected void createPreparedStatement(Connection db) throws SQLException {
    wrappedTableQuerier.createPreparedStatement(db);
  }

  @Override
  protected ResultSet executeQuery() throws SQLException {
    return wrappedTableQuerier.executeQuery();
  }

  /**
   * Reads all data sets and groups them until the value of the grouping columns changes
   * @return  the master/detail record
   * @throws SQLException if there is a problem reading from the database
   */
  @Override
  public SourceRecord extractRecord() throws SQLException {
    log.debug("BEGIN -----------------------------------------------");

    SourceRecord firstRecord = nextRecord;
    Map<String, ?> sourceOffset = firstRecord.sourceOffset();

    log.debug("MasterDetailTableQuerier: extractRecord data set: {}", firstRecord.value());

    // create and fill new master record
    final Struct master = extractColumns(firstRecord, masterSchemaMapping);
    final List<Struct> detail = new ArrayList<>();

    // add first detail to the detail-Struct
    detail.add(extractColumns(firstRecord, detailSchemaMapping));
    log.debug("MasterDetailTableQuerier: added first detail record");

    final Struct previousGroupingValues = extractColumns(firstRecord, groupingSchemaMapping);

    // while has next() and grouping cols content didn't change
    boolean hadNext;
    while ((hadNext = next()) && previousGroupingValues
            .equals(extractColumns(nextRecord, groupingSchemaMapping))) {
      log.debug("MasterDetailTableQuerier: next data set: {}", nextRecord.value());
      // add detail to detail-Struct
      detail.add(extractColumns(nextRecord, detailSchemaMapping));
      sourceOffset = nextRecord.sourceOffset();
      log.debug("MasterDetailTableQuerier: added detail record");
    }

    // finalize current SourceRecord
    master.put(detailName, detail);
    log.debug("MasterDetailTableQuerier: final master record {}", master);
    log.debug("END -----------------------------------------------");

    // remember to skip next()
    skipNext = hadNext ? NextState.SKIP : NextState.END;

    return new SourceRecord(
            firstRecord.sourcePartition(),
            sourceOffset,
            firstRecord.topic(),
            firstRecord.kafkaPartition(),
            null, null,
            master.schema(),
            master,
            firstRecord.timestamp(),
            firstRecord.headers());
  }

  /**
   * Extracts the fields defined by the parameter {@param schemaMapping} from
   * the extracted record (i.e. from the underlying querier.
   *
   * @param extractedRecord the {@link SourceRecord} from which the values are taken
   * @param schemaMapping the {@link SchemaMapping} to be used for creating the Struct
   * @return a {@link Struct} holding the values extracted from current resultSet
   */
  protected Struct extractColumns(SourceRecord extractedRecord, SchemaMapping schemaMapping) {
    Struct record = new Struct(schemaMapping.schema());
    Struct values = (Struct)extractedRecord.value();
    // the schema used for record is no the same as the one in extractedRecord and hence
    // for extracting the value we use the field name rather than the field
    // in addition we need to exclude the filed later holding the detail records
    record.schema().fields().stream().filter(field ->
            !detailName.equals(field.name())).forEach(field ->
            record.put(field, values.get(field.name())));
    return record;
  }

  @Override
  public int compareTo(TableQuerier other) {
    return wrappedTableQuerier.compareTo(other);
  }

  @Override
  public long getLastUpdate() {
    return wrappedTableQuerier.getLastUpdate();
  }

  @Override
  public PreparedStatement getOrCreatePreparedStatement(Connection db) throws SQLException {
    return wrappedTableQuerier.getOrCreatePreparedStatement(db);
  }

  @Override
  public boolean querying() {
    return wrappedTableQuerier.querying();
  }

  /**
   * Moves the ResultSet to the next available data set. There are operations which may already
   * have progressed the ResultSet and hence calling next() will return true/false and skip the
   * progression. In this case the ResultSet will still contain the previous data set.
   * @return true if there is a new data set available.
   * @throws SQLException if a database access error occurs or this method is
   *         called when the internal result set was closed already
   */
  @Override
  public boolean next() throws SQLException {
    switch (skipNext) {
      case SKIP:
        log.debug("MasterDetailTableQuerier: skip next - SKIP");
        skipNext = NextState.NEXT;
        return true;
      case END:
        log.debug("MasterDetailTableQuerier: skip next - END");
        skipNext = NextState.NEXT;
        return false;
      case NEXT:
        log.debug("MasterDetailTableQuerier: skip next - NEXT");
        if(wrappedTableQuerier.next()) {
          nextRecord = wrappedTableQuerier.extractRecord();
          log.debug("MasterDetailTableQuerier: skip next - NEXT record {}", nextRecord.value());
          return true;
        }
        nextRecord = null;
        return false;
      default:
        throw new ConnectException("Unexpected skipNext: " + skipNext);
    }
  }

  @Override
  public int getAttemptedRetryCount() {
    return wrappedTableQuerier.getAttemptedRetryCount();
  }

  @Override
  public void incrementRetryCount() {
    wrappedTableQuerier.incrementRetryCount();
  }

  @Override
  public void resetRetryCount() { wrappedTableQuerier.resetRetryCount(); }

  @Override
  public String toString() {
    return "MasterDetailTableQuerier{"
            + "table=" + tableId
            + ", query='" + query + '\''
            + ", topicPrefix='" + topicPrefix + '\''
            + ", groupingColumns='" + (groupingColumns != null ?
                                       groupingColumns : "") + '\''
            + ", wrappedQuerier=" + wrappedTableQuerier
            + '}';
  }

}