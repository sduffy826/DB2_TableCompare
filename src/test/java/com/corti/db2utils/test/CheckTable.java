package com.corti.db2utils.test;

import java.sql.*;
import java.io.BufferedWriter;

public class CheckTable {
  Connection dbConn = null;

  protected CheckTable() {
    super();
  }

  CheckTable(Connection _target) {
    super();
    dbConn = _target;
  }

  public void dumpSeqRev(int num2write) throws Exception {
    // prepare the query statement
    // String query =
    // "SELECT * FROM BRSGUIDE.BRS_CONTRACTS ORDER BY 1,2";

    String query = "SELECT * FROM BRS.CONTRACT "
        + " WHERE SEQUENCE = 'AA19405' ORDER BY 1,2";

    PreparedStatement stmt = dbConn.prepareStatement(query);

    int numgot = 0;
    // execute the query
    ResultSet rs = stmt.executeQuery();
    while (rs.next() & numgot < num2write) {
      numgot++;

      System.out.println("Got seq " + rs.getString(1) + " rev " + rs.getInt(2)
          + " name " + rs.getString(9));
    }

    dbConn.commit();
    return;
  }

  private String getDistinct(String _tbCreator, String _tbName, String _colName)
      throws Exception {
    String query = "SELECT DISTINCT(" + _colName + ") FROM " + _tbCreator + "."
        + _tbName;

    PreparedStatement stmt = dbConn.prepareStatement(query);

    String result = "";
    // execute the query
    ResultSet rs = stmt.executeQuery();
    while (rs.next()) {
      result += rs.getString(1) + ",";
    }
    rs.close();
    stmt.close();

    return result;
  }

  private String getMaxLen(String _tbCreator, String _tbName, String _colName)
      throws Exception {
    String query = "SELECT MAX(LENGTH(" + _colName + ")) FROM " + _tbCreator
        + "." + _tbName;

    PreparedStatement stmt = dbConn.prepareStatement(query);

    int theLen = 0;
    // execute the query
    ResultSet rs = stmt.executeQuery();
    if (rs.next()) {
      theLen = rs.getInt(1);
    }
    rs.close();
    stmt.close();

    return Integer.toString(theLen);
  }

  private String getRec(String _tbCreator, String _tbName, String _colName)
      throws Exception {
    String query = "SELECT " + _colName + " FROM " + _tbCreator + "." + _tbName;

    PreparedStatement stmt = dbConn.prepareStatement(query);

    String theStr;
    // execute the query
    ResultSet rs = stmt.executeQuery();
    if (rs.next()) {
      theStr = rs.getString(1);
    } else {
      theStr = " ";
    }
    rs.close();
    stmt.close();

    return theStr;
  }

  public void dumpColumns(String _tbCreator, String _tbName,
      BufferedWriter _out, String _regex) throws Exception {

    boolean writeFile = (_out != null); // ? true : false;
    boolean matches;
    String delim = writeFile ? "," : " ";
    String sufx;

    String query = "SELECT NAME, COLNO, COLTYPE, LENGTH, SCALE, NULLS FROM "
        + "SYSIBM.SYSCOLUMNS WHERE TBCREATOR = '" + _tbCreator + "' AND "
        + "TBNAME = '" + _tbName + "' ORDER BY COLNO";

    PreparedStatement stmt = dbConn.prepareStatement(query);

    // execute the query
    ResultSet rs = stmt.executeQuery();
    while (rs.next()) {
      String colName = rs.getString(1);
      matches = colName.matches(_regex);
      if (matches) {
        sufx = getDistinct(_tbCreator, _tbName, colName);
      } else {
        sufx = "";
      }
      String outRec = "Col" + delim + colName + delim + "ColNo" + delim
          + rs.getInt(2) + delim + "ColType" + delim + rs.getString(3) + delim
          + "Length" + delim + rs.getInt(4) + delim + "Scale" + delim
          + rs.getInt(5) + delim + "Nulls" + delim + rs.getString(6) + delim
          + getMaxLen(_tbCreator, _tbName, colName) + delim + sufx;

      System.out.println(outRec);

      if (writeFile) {
        _out.write(outRec);
        _out.newLine();
      }
    }
    rs.close();
    stmt.close();
    dbConn.commit();
    return;
  }

  public void showMetaData(String _owner, String _table) throws Exception {
    String query = "SELECT * FROM " + _owner + "." + _table;

    PreparedStatement stmt = dbConn.prepareStatement(query);

    // execute the query
    ResultSet rs = stmt.executeQuery();
    ResultSetMetaData rsmd = rs.getMetaData();
    int numberOfColumns = rsmd.getColumnCount();

    System.out.println("getSchemaName: " + rsmd.getSchemaName(1));
    System.out.println("getTableName: " + rsmd.getTableName(1));
    System.out.println("");

    for (int i = 1; i <= numberOfColumns; i++) {
      System.out.println("getColumnLabel: " + rsmd.getColumnLabel(i));
      System.out.println("getColumnName: " + rsmd.getColumnName(i));
      System.out.println("getColumnType: " + rsmd.getColumnType(i));
      System.out.println("getColumnTypeName: " + rsmd.getColumnTypeName(i));
      System.out.println("getPrecision: " + rsmd.getPrecision(i));
      System.out.println("getScale: " + rsmd.getScale(i));
      System.out.println("");
    }
    dbConn.commit();
    return;
  }
}
