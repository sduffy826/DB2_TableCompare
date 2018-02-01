package com.corti.db2utils.test;

import java.util.ArrayList;
import java.util.List;
import java.sql.Connection;
import java.util.logging.Logger;

import com.corti.db2utils.*;
import com.corti.javalogger.LoggerUtils;

/*
 * Class to compare tables in different schemas; the first schema argument is used to search for
 * tables in the other one; if the table is found then it'll be compared.  For example say you have
 * an 'archive' schema that has a subset of tables in the 'base' schema; then you'd want to pass
 * the 'archive' as the first set of arguments and the 'base' as the second... that way each table
 * in 'archive' will be looked at agains the 'base'.   
 */
public class CompareTablesInDiffSchemas {
  private static Logger logger = null;
  private static final boolean showTables = false;
  private static final boolean reportSameStructure = false;
  
  private String schema1;
  private String schema2;  
  
  private Connection db2Connection1;
  private Connection db2Connection2;
  
  // Mainline
  public static void main(java.lang.String[] args) {
    CompareTablesInDiffSchemas app = new CompareTablesInDiffSchemas();              
    app.run(args);       
  }
  
  // Validate arguments passed in are good, and if so init vars
  private boolean checkArgs(String[] args) {
    boolean returnValue = false;
    if (args.length < 8)  
      logger.severe("Must pass odbcName1 uid1 pw1 schema1 odbcName2 uid2 pw2 schema2");
    else {
      // odbcName, userid, pw (args 0->2)
      db2Connection1 = Db2Connections.getInstance().getConnection(args[0],args[1],args[2]);
      schema1 = args[3];
      
      db2Connection2 = Db2Connections.getInstance().getConnection(args[4],args[5],args[6]);
      schema2 = args[7];
      returnValue = true;
    }
    return returnValue;
  }
  
  // The main logic of the class :)
  private void run(String[] args) {  
    logger = (new LoggerUtils()).getLogger("DB2TableCompare", "CompareTablesInDiffSchemas", true);
    if (checkArgs(args)) {
      
      // Get the table listings
      GetSchemaTables tablesInSchema1 = new GetSchemaTables(logger, db2Connection1, schema1);
      
      GetSchemaTables tablesInSchema2 = new GetSchemaTables(logger, db2Connection2, schema2);
      
      List<String> myList = tablesInSchema1.getTables();
  
      // If want to show the tables in the first list then write em out :)
      if (showTables) {
        for (String aTable : myList) {
          logger.info("List of tables in schema");
          logger.info(aTable);
        }
      }
      
      // Iterate over list of tables, if it exists in second set then do comparison
      for (String aTable : myList) {
        if (tablesInSchema2.hasTable(aTable)) {
          // Table exists in both places, compare them
          Db2TableColumns table1 = new Db2TableColumns(db2Connection1, schema1, aTable);
          Db2TableColumns table2 = new Db2TableColumns(db2Connection2, schema2, aTable);
          String tableDiff = table1.showDiffColumns(table2);
          if (tableDiff.isEmpty() == false) {
            logger.info("** Table: " + aTable + " different in schemas");
            logger.info("  " + tableDiff + "\n");
            logger.info(table1.toCsvString());
            logger.info(" ");
            logger.info(table2.toCsvString());
          }
          else {
            if (reportSameStructure) logger.info("Table: " + aTable + " is the same in schemas");
          }          
        }
        else {
          logger.info("  Table not in other schema: " + aTable );
        }
      }
    }
  }
}