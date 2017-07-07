package com.corti.db2utils;

import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

import com.corti.javalogger.LoggerUtils;

public class GetTables {
  private static final boolean DEBUGIT = false;

  Connection dbConn = null;
  String odbc = null;
  String uid = null;
  String pwd = null;
  String schema = null;
  Logger logger = null;

  // Hold list of tables
  List<Db2Table> tableListing = null;
  
  // Lookup for tables
  Map<String, Db2Table> tableLookup = null;

  protected GetTables() {
    super();
  }

  /**
   * Constructor, pass in logger object (can be null), the odbc connect, the userid, pw and
   * schema; if schema is null or "" it'll look at all tables, other than SYS* ones, otherwise
   * it'll only compare tables with that schema
   * 
   * @param _logger java.util.logging.Logger object, or can be null
   * @param _odbc odbc connection string
   * @param _uid userid with connect/select auth
   * @param _pwd pw for userid
   * @param _schema schema to look at (or null, "" for all non system tables)
   */
  public GetTables(Logger _logger, String _odbc, String _uid, String _pwd, String _schema) {
    super();

    this.odbc = _odbc;
    this.uid = _uid;
    this.pwd = _pwd;
    this.schema = ( _schema != null ? _schema.trim() : "");
    if (_logger != null) {
      this.logger = _logger;
    }
    else {
      this.logger = (new LoggerUtils()).getLogger("DB2TableCompare", "DB2TableCompareLogger", true);
    }

    tableListing = new ArrayList<Db2Table>();
    tableLookup = new HashMap<String, Db2Table>();
    int numTables = getTables();
    if (DEBUGIT) {
      logger.info("Got " + numTables + ".");
      dumpTables();
    }
  }

  /**
   * Routine to build the query; if schema was defined then only look at tables for that
   * schema, otherwise look at all tables that aren't in the SYS* schemas
   * @return
   */
  private String buildQuery() {
    String query;
    if (schema.length() == 0) {
      query = "select tabschema, tabname, type, colcount "
          + "from syscat.tables "
          + "where tabschema not like 'SYS%' order by 1, 2";
    } else {
      query = "select tabschema, tabname, type, colcount "
          + "from syscat.tables " + "where tabschema = '" + schema
          + "' order by 2";
    }
    if (DEBUGIT)
      logger.info("Query: " + query);
    return query;
  }

  /**
   * @return Number of tables 
   */
  private int getTables() {
    logger.info("In default (private) constructor");
    int numgot = 0;
    try {
      // Get database connection
      dbConn = getConnection(odbc, uid, pwd);

      // Build query to use
      String query = buildQuery();

      PreparedStatement stmt;
      stmt = dbConn.prepareStatement(query);

      // execute the query
      ResultSet rs = stmt.executeQuery();
      while (rs.next()) {
        numgot++;

        // Select is schema, tablename, columncount
        Db2Table db2Tab = new Db2Table(rs.getString(1), rs.getString(2), rs.getString(3), rs.getInt(4));
        tableListing.add(db2Tab);
        tableLookup.put(Db2Table.getKey(db2Tab), db2Tab);

        if (DEBUGIT)
          logger.info(db2Tab.toString());
      }
      rs.close();
      stmt.close();
      dbConn.commit();

      // Loop thru and get record count for each table
      for (Db2Table db2Tab : tableListing) {
        if (db2Tab.isTable()) {
          db2Tab.setRecordCount(getRecordCount(dbConn, db2Tab.getSchema(), db2Tab.getName()));
        }
      }

      dbConn.close();
    } catch (SQLException e) {
      e.printStackTrace();
    }
    logger.info("Out of the private constructor");
    return numgot;
  }

  /**
   * Get the number of records for the table passed in
   * @param _dbConn The database connection (must exist)
   * @param _schema The table schema
   * @param _tableName Table name
   * @return Number of records in the table
   */
  public int getRecordCount(Connection _dbConn, String _schema, String _tableName) {
    int recCount = 0;
    String query = "select count(*) from " + _schema.trim() + "." + _tableName;

    PreparedStatement stmt;

    try {
      stmt = _dbConn.prepareStatement(query);
      ResultSet rs = stmt.executeQuery();
      while (rs.next()) {
        recCount = rs.getInt(1);
      }
      rs.close();
      stmt.close();
      dbConn.commit();
    } catch (SQLException e) {
      e.printStackTrace();
    }

    if (DEBUGIT)
      logger.info(_schema.trim() + "." + _tableName.trim() + " recCount: " + recCount);

    return recCount;
  }

  /**
   * Method for debuggin, it dumps out the table information for all tables
   */
  public void dumpTables() {
    logger.info("Dumping tableListing");
    for (Db2Table theTable : tableListing) {
      logger.info(theTable.toCSV());
    }

    // Dump keys of map out
    logger.info("Dumping tableLookup keys");
    for (String key : tableLookup.keySet()) {
      logger.info("\"Key: " + key + "\"");
    }

    return;
  }

  /**
   * Return database connection for the args passed in
   * @param odbc The odbc connection string
   * @param uid User id with connect auth
   * @param pwd Password for user
   * @return The database connection
   */
  private Connection getConnection(String odbc, String uid, String pwd) {
    Connection dbConnection = null;
    try {
      dbConnection = DriverManager.getConnection("jdbc:db2:" + odbc, uid, pwd);
    } 
    catch (SQLException e) {
      e.printStackTrace();
    }
    logger.info("Connected to " + odbc + " using userid " + uid);
    return dbConnection;
  }

  /**
   * Returns the table looup map
   * @return Map with the tables
   */
  public Map<String, Db2Table> getMap() {
    return tableLookup;
  }

  /**
   * Method to perform the table compares
   * @param _otherTables The other 'GetTables' object
   */
  public void compareTables(GetTables _otherTables) {
    Map<String, Db2Table> otherMap = _otherTables.getMap();

    logger.info("Comparing tables");
    for (Map.Entry<String, Db2Table> entry : tableLookup.entrySet()) {
      Db2Table myObj = entry.getValue();
      Db2Table otherObj = otherMap.get(entry.getKey());
      if (otherObj == null) {
        logger.info("\"In first db not second\",," + myObj.toCSV());
      } else {
        String outCompareMsg = myObj.compareCSV(otherObj);
        if (outCompareMsg != null) logger.info(outCompareMsg);
      }
    }

    // Now check for records in second db not in the first
    for (Map.Entry<String, Db2Table> entry : _otherTables.getMap().entrySet()) {
      Db2Table otherObj = entry.getValue();
      Db2Table myObj = tableLookup.get(entry.getKey());
      if (myObj == null) {
        logger.info("\"In second db not first:\",," + otherObj.toCSV());
      }
    }
  }
}
