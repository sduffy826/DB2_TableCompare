package com.corti.db2utils;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/*
 * Class represents the column attributes for a particular table
 */
public class Db2TableColumns {
  private List<Db2TableColumn> db2Columns;
  private Map<String, Db2TableColumn> db2ColumnLookup;
  private String schema;
  private String tableName;
  private Connection db2Connection;
  
  public Db2TableColumns(Connection db2Connection, String schema, String tableName) {
    this.db2Connection = db2Connection;
    this.schema        = schema.trim();
    this.tableName     = tableName.trim();
    init();    
  }
  
  public List<Db2TableColumn> getColumns() {
    return new ArrayList<Db2TableColumn>(db2Columns);
  }
  
  public Db2TableColumn getTableColumn(String columnName) {
    return db2ColumnLookup.get(columnName);
  }
  
  public void init() {
    db2Columns = new ArrayList<Db2TableColumn>();
    db2ColumnLookup = new HashMap<String, Db2TableColumn>();
    String query = buildQuery();

    PreparedStatement stmt;
    try {
      stmt = db2Connection.prepareStatement(query);

      // execute the query
      ResultSet rs = stmt.executeQuery();
      while (rs.next()) {
        Db2TableColumn db2TableColumn = new Db2TableColumn(rs.getString(1), // tablenName
            rs.getString(2), // schema
            rs.getString(3), // column name
            rs.getInt(4), // column number
            rs.getString(5), // nulls flag
            rs.getString(6), // column type
            rs.getInt(7), // column length
            rs.getInt(8) // column length scale
        );
        db2Columns.add(db2TableColumn);
        db2ColumnLookup.put(db2TableColumn.getColumnName(), db2TableColumn);
      }
      rs.close();
      stmt.close();      
    } catch (SQLException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
  }
  
  // Return returns true if table structures are different, we call other method to do
  // the work
  public boolean hasDiffColumns(Db2TableColumns otherTable) {
    return (showDiffColumns(otherTable).isEmpty() == false);     
  }
  
  // Return String that shows the table differences, empty string if no changes
  public String showDiffColumns(Db2TableColumns otherTable) {
    StringBuffer diffData = new StringBuffer();
    
    // Go thru loop twice    
    for (int i = 0; i < 2; i++) {
      Db2TableColumns first, second;
      if (i == 0) {
        first = this;
        second= otherTable;
      }
      else {
        first = otherTable;
        second = this;
      }
      
      List<Db2TableColumn> firstList = first.getColumns();
      for (Db2TableColumn firstRow : firstList) {
        Db2TableColumn otherRow = second.getTableColumn(firstRow.getColumnName());
        if (otherRow == null) {
          diffData.append("\nNot in other table: " + firstRow.toString());
        }
        else if (firstRow.almostMatchesOtherRow(otherRow) == false) {
          if (i == 0) diffData.append("\nDiff attributes: " + firstRow.toString() + 
                                      "\n" + otherRow.toString() + "\n");
        }
      }     
    }
    return diffData.toString();
  }  
  
  // Return string for the query to get columns
  private String buildQuery() {
    return "select tbname, tbcreator, name, colno, nulls, coltype, length, scale" +
           " from sysibm.syscolumns where tbcreator = '" + schema.toUpperCase() + "'" +
           " and tbname = '" + tableName.toUpperCase() + "' order by colno";
  }
    
  // Return table attributes as a string (each row on own line)
  public String toString() {
    StringBuffer newString = new StringBuffer();
    for (Db2TableColumn db2TableColumn : db2Columns) {
      newString.append(db2TableColumn.toString() + "\n");
    }
    return newString.toString();    
  }
  
  // Return table attributes as a csv string (each row on own line)
  public String toCsvString() {
    StringBuffer newString = new StringBuffer();
    for (Db2TableColumn db2TableColumn : db2Columns) {
      newString.append(db2TableColumn.toCsvString() + "\n");
    }
    return newString.toString();    
  }  
}
