package com.corti.db2utils;
import java.text.DecimalFormat;

/**
 * Class represents a DB2 Table
 * @author sduffy
 */
public class Db2Table {
  private double pctThreshold = 10.0;  // If over this amt then indicator displayed in output (*)
  private static final boolean SHOWEVENIFNOCHANGE = true;
  private String schema;
  private String name;
  private String type;
  private int colCount;
  private int recordCount;    
  
  /**
   * Return unique identifer for table, the schema and tablename concatenated (with delim)
   * @param _db2Table Table to get the unique identifier for
   * @return String representing unique id (SCHEMA~TABLENAME)
   */
  public static String getKey(Db2Table _db2Table) {
    return getKey(_db2Table.getSchema(), _db2Table.getName());
  }
 
  /**
   * Return the unique identifier for the table, the schema and tablename name
   * it is in format SCHEMA~TABLENAME (yep uppercased)
   * @return Unique identifier for table (SCHEMA~TABLNAME)
   */
  public static String getKey(String _schema, String _table) {
    return _schema.trim().toUpperCase() + "~" + _table.trim().toUpperCase();
  }
  
  /**
   * Default constructor
   */
  public Db2Table() {
    super();
  }
  
  
  /**
   * Constructor
   * @param schema Table schema
   * @param name Table name
   * @param type Table type (from syscat.tables)
   * @param colCount Number of columns in the table
   */
  public Db2Table(String schema, String name, String type, int colCount) {
    super();
    this.schema = schema;
    this.name = name;
    this.type = type;
    this.colCount = colCount;
    this.recordCount = 0;
  }
  
  /**
   * Method to compare two table objects and return differences as a string
   * @param _otherObj Other table to compare to
   * @return String of differences, null if no changes found
   */
  public String compare(Db2Table _otherObj) {
    return compareHelper(_otherObj,false);
  }
  
  /**
   * Method to compare two table objects and return differences as a csv string
   * @param _otherObj Other table to compare to
   * @return CSV formatted string, null if no changes found
   */
  public String compareCSV(Db2Table _otherObj) {
    return compareHelper(_otherObj,true);
  }
  
  /**
   * Helper method to compare two objects, it will return a string in csv format or
   * regular text string (based on arg).  If there aren't any differences then a null
   * string is returned.
   * @param _otherObj The other table to compare with this one
   * @param _wantCSVOutput Boolean true if you want csv output
   * @return String identifying the deltas
   */
  private String compareHelper(Db2Table _otherObj, boolean _wantCSVOutput) {
    String colCountPrefix = getColCountDeltaPrefix(_otherObj);
    double pctDelta = getRecordDeltaPct(_otherObj);
    String pctPrefix = getPctPrefix(pctDelta);
    String pctFormatted = formatPercentage(pctDelta);

    // Report if colCountPrefix exist, or pctDelta > 0 or record count = 0
    boolean shouldReport = ((colCountPrefix.trim().length() > 0)
        || (pctDelta > 0.0000) || (getRecordCount() == 0));

    // Build return string
    String rtnString = null;
    if (shouldReport || SHOWEVENIFNOCHANGE) {
      if (_wantCSVOutput) {
        rtnString = "`" + colCountPrefix + "`," + "`" + pctPrefix + "`,"
            + this.toCSV() + "," + "`other table colCount`,"
            + _otherObj.getColCount() + "," + "`recordCount`,"
            + _otherObj.getRecordCount() + "," + "`pctDiff`," + pctFormatted;
        return rtnString.replaceAll("`", "\"");
      } else {
        rtnString = colCountPrefix + pctPrefix + " " + this.toString() + " "
            + "other table colCount: " + _otherObj.getColCount() + " "
            + "recordCount: " + _otherObj.getRecordCount() + " " + "pctDiff: "
            + pctFormatted;
      }
    }
    return rtnString;
  }
  
  /**
   * Format the percentage as a 'nice' decimal number n.nnnn (number passed in should be 10.0 for
   * percentage, not 0.10)
   * 
   * @param _percentage
   * @return Formatted string as decimal with 4 digit precision
   */
  private String formatPercentage(double _percentage) {
    return new DecimalFormat("##0.0000").format(_percentage);
  }
  
  /**
   * @return Number of columns in table
   */
  public int getColCount() {
    return colCount;
  }
  
  /**
   * Return a prefix of ! if the table column counts differ, done to flag important differences
   * to user.
   * 
   * @return Prefix value of ! or whitepace
   */
  private String getColCountDeltaPrefix(Db2Table _otherObj) {
    return (getColCount() != _otherObj.getColCount() ? "!" : " ");
  }
 
  /**
   * @return Name of the table
   */
  public String getName() {
    return name;
  }
  
  /**
   * Method returns an indicator that can be used as a 'prefix' to flag records where the
   * percentage (usually of record count differences) are over the threshold (constant) percentage.
   * Done to flag records that should be looked at.
   * 
   * @param _thePct
   * @return A string 'prefix' indicator
   */
  private String getPctPrefix(double _thePct) {
    return (_thePct > pctThreshold ? "*" : " ");
  }
  
  /**
   * Return percent threshold, if tables have record counts that exceed this threshold percent
   * then an indicator will be displayed in the output (*); done to flag records to be looked at.
   * <p>Note the percent here is format 10.0 for 10.0%, not stored as 0.10</p>
   * @return Percent threshold
   */
  public double getPctThreshold() {
    return pctThreshold;
  }
  
  /**
   * @return Number of records in the table
   */
  public int getRecordCount() {
    return recordCount;
  } 

  /**
   * Calculates the percentage differences between two tables record counts.
   * @param _otherObj
   * @return Percentage (i.e. 10.0 is 10.0% (not 0.10))
   */
  private Double getRecordDeltaPct(Db2Table _otherObj) {
    double percentage = 100.00;
    if (getRecordCount() != _otherObj.getRecordCount()) {      
      int deltaCnt = Math.abs(getRecordCount() - _otherObj.getRecordCount());
      if (getRecordCount() > 0) {
        percentage = ((deltaCnt * 100.0) / getRecordCount());
      }      
    }
    else percentage = 0.0; // No diff
    return percentage;
  }
  
  /**
   * @return Schema
   */
  public String getSchema() {
    return schema;
  }
  
  /**
   * @return Table type (i.e. 'T' - Table, 'V' - View, etc... (from syscat.tables))
   */
  public String getType() {
    return type;
  }
  
  /**
   * @return Indicator as to whether artifact is a table or not
   */
  public boolean isTable() {
    return (type.equalsIgnoreCase("T"));
  }
  
  /**
   * Set column count for the table
   * @param colCount
   */
  public void setColCount(int colCount) {
    this.colCount = colCount;
  }
  
  /**
   * Set table name
   * @param name
   */
  public void setName(String name) {
    this.name = name;
  }
  
  /**
   * Return percent threshold, if tables have record counts that exceed this threshold percent
   * then an indicator will be displayed in the output (*); done to flag records to be looked at.
   * <p>Note the percent here is format 10.0 for 10.0%, not stored as 0.10</p>
   * @return Percent threshold
   */
  public void setPctThreshold(double pctThreshold) {
    this.pctThreshold = pctThreshold;
  }
  
  
  /**
   * Set record count for the table
   * @param recordCount
   */
  public void setRecordCount(int recordCount) {
    this.recordCount = recordCount;
  }
  
  /**
   * Set schema name
   * @param schema
   */
  public void setSchema(String schema) {
    this.schema = schema;
  }
  
  /**
   * Set type of table (i.e. 'T'-Table, 'V'-View)
   * @param type
   */
  public void setType(String type) {
    this.type = type;
  }
  
  /**
   * @return Return elements in csv format "name",value,"name",value...
   */
  public String toCSV() {
    String rtnString = "`schema`,`" + schema + "`" +
                       ",`name`,`" + name +  "`" +
                       ",`type`,`" + type + "`" +
                       ",`colCount`," + colCount + 
                       ",`recCount`," + recordCount;
    return rtnString.replaceAll("`", "\"");
  }

  /**
   * @return Return string representation of object
   */
  @Override
  public String toString() {
    return "Db2Table [schema=" + schema + ", name=" + name + ", colCount="
        + colCount + ", recCount: " + recordCount + "]";
  }
}
