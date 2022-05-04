package io.confluent.connect.jdbc.util;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * This filter is wrapping an {@code ResultSetMetaData} object. Only columns specified will be
 * visible and accessible through this filter. The filter behaves like the filtered columns
 * would not exist.
 * <p>
 * Main usage is to create several "lenses" for the {@code MasterDetailTableQuerier} where
 * columns can be specified to appear on the master record and columns to appear on the detail
 * columns. The filter is necessary for setting up a Schema where {@code SchemaMapping} is
 * including all columns it can find in the {@code ResultSetMetaData} object.
 */
public class ResultSetMetaDataFilter implements ResultSetMetaData {

  private final ResultSetMetaData internalResultSetMetaData;
  private final Map<Integer,Integer> columnMapping;

  /**
   * Instantiates a new {@code ResultSetMetaDataFilter} object which wraps a MetaData object and
   * filters all columns not contained in {@code columns}
   * @param resultSetMetaData the metadata object to be filtered
   * @param columns           the columns which should be visible through this filter - all other
   *                          columns won't be accessible nor visible
   * @throws SQLException if a column specified is not available in the {@code resultSetMetaData}
   */
  public ResultSetMetaDataFilter(ResultSetMetaData resultSetMetaData, List<String> columns)
          throws SQLException {
    this.internalResultSetMetaData = resultSetMetaData;
    this.columnMapping = setupColumnMapping(resultSetMetaData, columns);
  }

  static Map<Integer,Integer> setupColumnMapping(final ResultSetMetaData resultSetMetaData,
                                                 final List<String> columns)
          throws SQLException {
    final Map<Integer,Integer> colMapping = new HashMap<>();
    if (columns.size() == 0) {
      // if no columns specified, we create a 1:1 mapping;
      for (int i = 0; i < resultSetMetaData.getColumnCount(); i++) {
        colMapping.put(i+1, i+1);
      }
      return colMapping;
    }
    for (int i = 0; i < columns.size() ; i++) {
      colMapping.put(i+1, findColumn(resultSetMetaData, columns.get(i)));
    }
    return colMapping;
  }

  static Integer findColumn(final ResultSetMetaData resultSetMetaData, final String columnName)
          throws SQLException {
    for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
      if (resultSetMetaData.getColumnName(i).equals(columnName)) {
        return i;
      }
    }
    throw new SQLException("Column name '" + columnName + "' does not exist in "
            + resultSetMetaData);
  }

  @Override
  public int getColumnCount() {
    return columnMapping.size();
  }

  @Override
  public boolean isAutoIncrement(int column) throws SQLException {
    return internalResultSetMetaData.isAutoIncrement(columnMapping.get(column));
  }

  @Override
  public boolean isCaseSensitive(int column) throws SQLException {
    return internalResultSetMetaData.isCaseSensitive(columnMapping.get(column));
  }

  @Override
  public boolean isSearchable(int column) throws SQLException {
    return internalResultSetMetaData.isSearchable(columnMapping.get(column));
  }

  @Override
  public boolean isCurrency(int column) throws SQLException {
    return internalResultSetMetaData.isCurrency(columnMapping.get(column));
  }

  @Override
  public int isNullable(int column) throws SQLException {
    return internalResultSetMetaData.isNullable(columnMapping.get(column));
  }

  @Override
  public boolean isSigned(int column) throws SQLException {
    return internalResultSetMetaData.isSigned(columnMapping.get(column));
  }

  @Override
  public int getColumnDisplaySize(int column) throws SQLException {
    return internalResultSetMetaData.getColumnDisplaySize(columnMapping.get(column));
  }

  @Override
  public String getColumnLabel(int column) throws SQLException {
    return internalResultSetMetaData.getColumnLabel(columnMapping.get(column));
  }

  @Override
  public String getColumnName(int column) throws SQLException {
    return internalResultSetMetaData.getColumnName(columnMapping.get(column));
  }

  @Override
  public String getSchemaName(int column) throws SQLException {
    return internalResultSetMetaData.getSchemaName(columnMapping.get(column));
  }

  @Override
  public int getPrecision(int column) throws SQLException {
    return internalResultSetMetaData.getPrecision(columnMapping.get(column));
  }

  @Override
  public int getScale(int column) throws SQLException {
    return internalResultSetMetaData.getScale(columnMapping.get(column));
  }

  @Override
  public String getTableName(int column) throws SQLException {
    return internalResultSetMetaData.getTableName(columnMapping.get(column));
  }

  @Override
  public String getCatalogName(int column) throws SQLException {
    return internalResultSetMetaData.getCatalogName(columnMapping.get(column));
  }

  @Override
  public int getColumnType(int column) throws SQLException {
    return internalResultSetMetaData.getColumnType(columnMapping.get(column));
  }

  @Override
  public String getColumnTypeName(int column) throws SQLException {
    return internalResultSetMetaData.getColumnTypeName(columnMapping.get(column));
  }

  @Override
  public boolean isReadOnly(int column) throws SQLException {
    return internalResultSetMetaData.isReadOnly(columnMapping.get(column));
  }

  @Override
  public boolean isWritable(int column) throws SQLException {
    return internalResultSetMetaData.isWritable(columnMapping.get(column));
  }

  @Override
  public boolean isDefinitelyWritable(int column) throws SQLException {
    return internalResultSetMetaData.isDefinitelyWritable(columnMapping.get(column));
  }

  @Override
  public String getColumnClassName(int column) throws SQLException {
    return internalResultSetMetaData.getColumnClassName(columnMapping.get(column));
  }

  @Override
  public <T> T unwrap(Class<T> iface) {
    return null;
  }

  @Override
  public boolean isWrapperFor(Class<?> iface) {
    return this.getClass().isAssignableFrom(iface);
  }
}
