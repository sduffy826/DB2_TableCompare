package com.corti.db2utils.test;

import java.util.logging.Logger;

import com.corti.db2utils.*;
import com.corti.javalogger.LoggerUtils;

/**
 * This is the main line program to compare tables in two different databases; when invoked you
 * should pass in the odbcString, userId, pw for the first db, then follow it by the same fields
 * for the scond database.
 * 
 * @author sduffy
 */
public class MainTest {
  private static final String JDBC_DRIVER = "COM.ibm.db2.jdbc.app.DB2Driver";
  private Logger logger = null;
  private GetTables getTables1 = null;
  private GetTables getTables2 = null;  
 
  /**
   * Starts the application.
   * @param args an array of command-line arguments
   */
  public static void main(java.lang.String[] args) {

    try {
      MainTest app = new MainTest();              
      
      // register the db2 jdbc driver with DriverManager
      Class.forName(JDBC_DRIVER).newInstance();

      // Run the application
      app.run(args);
    }
    catch (Exception e) {
      e.printStackTrace();
      System.exit(-1);
    }    
  }
  
  /**
   * This make sure the arguments are valid, if so it'll create the two GetTables objects for
   * the comparison.
   * 
   * @param args The array of arguments passed into the mainline program
   * @return True if args were valid and objects created, false otherwise
   */
  private boolean checkArgs(String[] args) {
    boolean returnValue = false;
    if (args.length != 6)
      logger.severe("Must pass odbcName1 userid1 password1  odbcName2 userid2 password2");
    else {
      logger.info("classpath is: " + System.getProperty("java.class.path"));
      logger.info("library path is: " + System.getProperty("java.library.path"));
      
      getTables1 = new GetTables(logger, args[0], args[1], args[2], "");
      getTables2 = new GetTables(logger, args[3], args[4], args[5], "");    
      
      returnValue = true;
    }
    return returnValue;
  }
  
  /**
   * Create object and get logger object
   */
  private MainTest() {
    super();
    logger = (new LoggerUtils()).getLogger("DB2TableCompare", "DB2TableCompareLogger", true);
  }
  
  /**
   * Run the main part of the application, really just comparing
   */
  private void run(String[] args) {
    if (checkArgs(args)) {
      // Call method to compare the tables
      getTables1.compareTables(getTables2);
    }
  }
}