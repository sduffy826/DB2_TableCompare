package com.corti.db2utils;

import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

import com.corti.javalogger.LoggerUtils;

public class Db2Connections {
  private static final boolean DEBUGIT = true;
  
  // private static final String JDBC_DRIVER = "COM.ibm.db2.jdbc.app.DB2Driver";
  private static final String JDBC_DRIVER = "com.ibm.db2.jcc.DB2Driver";
 
  private static Db2Connections me = null;
  private static Map<String, Connection> connections = null;
  private static Logger logger = null;
  
  // Make it so constructor isn't accessible publicly
  protected Db2Connections() {
    super();
    try {
      // register the db2 jdbc driver with DriverManager
      Class.forName(JDBC_DRIVER).newInstance();
    }
    catch (Exception e) {
      e.printStackTrace();
      System.exit(-1);
    }    
    
    if (DEBUGIT) System.out.println("In constructor for Db2Connections, driver loaded");
    
    connections = new HashMap<String, Connection>();
    logger = (new LoggerUtils()).getLogger("DB2TableCompare", "DB2Connections", true);
  }
  
  // Return object reference
  public static Db2Connections getInstance() {
    if ( me == null ) {
      me = new Db2Connections();
      logger.info("First call to Db2Connections");      
    }
    else if (DEBUGIT) logger.info("Db2Connections already initialized");
    return me;
  }
  
  // Return database connection for a given alias
  public Connection getConnection(String alias) {
    return connections.get(alias);
  }
    
  // Get the database connection, this one doesn't have the caller specifying the alias so we'll use
  // the odbc argument as the alias
  public Connection getConnection(String odbc, String uid, String pw) {
    return getConnection(odbc, odbc, uid, pw);      
  }
  
  // Get the database connection but this one supports an alias; really only need if you
  // want multiple connections to same odbc source
  public Connection getConnection(String alias, String odbc, String uid, String pw) {
    Connection theConnection = getConnection(alias);
    if (theConnection == null ) {
      theConnection = getDbConnection(odbc, uid, pw);
      connections.put(alias, theConnection);
      logger.info("Added connection to list of connections");
    }
    else logger.info("Connection to db already existed, reusing it");
    return theConnection;
  }
  
  /**
   * Return database connection for the args passed in
   * @param odbc The odbc connection string
   * @param uid User id with connect auth
   * @param pwd Password for user
   * @return The database connection
   */
  private Connection getDbConnection(String odbc, String uid, String pwd) {
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
}