package com.corti.db2utils.test;

import java.sql.Connection;
import java.util.logging.Logger;

import com.corti.db2utils.*;
import com.corti.javalogger.LoggerUtils;

public class GetTableInfo {
  private static Logger logger = null;
  
  private String schema;
  private String tableName;
  private String odbcName;
  private String userid;
  private String password;  
  private boolean isAHostTable;
  
  private Connection db2Connection = null;
  //
  public static void main(java.lang.String[] args) {
    GetTableInfo app = new GetTableInfo();              
    app.run(args);       
  }
  
  // Validate arguments passed in are good, and if so init vars
  private boolean checkArgs(String[] args) {
    boolean returnValue = false;
    if (args.length < 5)  
      logger.severe("Must pass odbcName userid password schema tableName ");
    else {
      odbcName     = args[0];
      userid       = args[1];
      password     = args[2];
      schema       = args[3];
      tableName    = args[4];
      isAHostTable = false;
      if ((args.length > 5) && (args[5].trim().equalsIgnoreCase("y"))) {
        System.out.println("Is a host table");

        isAHostTable = true;
      }
      
      returnValue = true;
    }
    return returnValue;
  }
  
  //
  private void run(String[] args) {
    logger = (new LoggerUtils()).getLogger("DB2TableCompare", "CheckTableInfo", true);
    if (checkArgs(args)) {
      db2Connection = Db2Connections.getInstance().getConnection(odbcName, userid, password);
      
      Db2TableColumns db2TableColumns = new Db2TableColumns(db2Connection, schema, tableName, isAHostTable);
      
      logger.info(db2TableColumns.toCsvString());
      
      if (db2TableColumns.hasDiffColumns(db2TableColumns)) logger.info("is Diff columns");
      else logger.info("columns are the same");      
    }
  }
}