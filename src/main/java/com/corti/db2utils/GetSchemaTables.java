package com.corti.db2utils;

import java.sql.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.logging.Logger;

import com.corti.javalogger.LoggerUtils;

public class GetSchemaTables {
  private static final boolean DEBUGIT = true;

  Connection dbConn = null;
  String schema = null;
  Logger logger = null;

  // Hold list of tables
  List<String> tableListing;
  
  // Lookup for tables
  Set<String> tableLookup;

  protected GetSchemaTables() {
    super();
  }

  public GetSchemaTables(Logger logger, Connection db2Connection, String schema) {
    super();

    this.dbConn = db2Connection;
    this.schema = schema.trim();
    if (logger != null) {
      this.logger = logger;
    }
    else {
      this.logger = (new LoggerUtils()).getLogger("DB2TableCompare", "GetSchemaTables", true);
    }

    tableListing = new ArrayList<String>();
    tableLookup = new HashSet<String>();
    
    initListOfTables();
  }

  private String buildQuery() {
    String query = "select tabname from syscat.tables " + 
                      "where tabschema = '" + schema.toUpperCase() + "' and " +
                            "type = 'T' order by 1";
    if (DEBUGIT)
      logger.info("Query: " + query);
    return query;
  }

  public List<String> getTables() {
    return new ArrayList<String>(tableListing);
  }
  
  public boolean hasTable(String tableName) {
    return tableLookup.contains(tableName);
  }
  
  private void initListOfTables() {
    try {
      String query = buildQuery();

      PreparedStatement stmt;
      stmt = dbConn.prepareStatement(query);

      // execute the query
      ResultSet rs = stmt.executeQuery();
      while (rs.next()) {
        tableListing.add(rs.getString(1));
        tableLookup.add(rs.getString(1));
      }
      rs.close();
      stmt.close();
      dbConn.commit();
    } catch (SQLException e) {
      e.printStackTrace();
    }    
    return;
  }

  /**
   * Method for debuggin, it dumps out the table information for all tables
   */
  public void dumpTables() {
    logger.info("Dumping tableListing");
    int idx = 0;
    for (String tableName : tableListing) {
      logger.info("Index: " + Integer.toString(++idx) + " " +  tableName);
    }
    return;
  }
}
