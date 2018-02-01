package com.corti.db2utils;

/*
 * Class represents one particular table column's attributes, the Db2TableColumns class has a collection
 * of these to represent one particular table
 */
public class Db2TableColumn {
  private String tableName;     // VarChar 128
  private String tableCreator;  // VarChar 128
  private String columnName;    // VarChar 128
  private int columnNumber;     // SmallInt
  private String columnNulls;   // Char 1
  private String columnType;    // Char 8
  private int columnLength;     // SmallInt
  private int columnScale;      // SmallInt
  
  private Db2TableColumn() { }

  // Force that this is the only constructor
  public Db2TableColumn(String tableName, String tableCreator,
      String columnName, int columnNumber, String columnNulls,
      String columnType, int columnLength, int columnScale) {
    super();
    this.tableName = tableName.trim();
    this.tableCreator = tableCreator.trim();
    this.columnName = columnName.trim();
    this.columnNumber = columnNumber;
    this.columnNulls = columnNulls.trim();
    this.columnType = columnType.trim();
    this.columnLength = columnLength;
    this.columnScale = columnScale;
  }

  public String getTableName() {
    return tableName;
  }

  public String getTableCreator() {
    return tableCreator;
  }

  public String getColumnName() {
    return columnName;
  }

  public int getColumnNumber() {
    return columnNumber;
  }

  public String getColumnNulls() {
    return columnNulls;
  }

  public String getColumnType() {
    return columnType;
  }

  public int getColumnLength() {
    return columnLength;
  }

  public int getColumnScale() {
    return columnScale;
  }
  
  // Return true if this records data matches the argument passed in; we don't compare
  // the tablename, or creator (for obvious reasons)
  public boolean matchesOtherRow(Db2TableColumn otherRow) {
    return ((this.getColumnName().compareTo(otherRow.getColumnName()) == 0) &&
            (this.getColumnNumber() == otherRow.getColumnNumber()) &&
            (this.getColumnNulls().compareTo(otherRow.getColumnNulls()) == 0) &&
            (this.getColumnType().compareTo(otherRow.getColumnType()) == 0) &&
            (this.getColumnLength() == otherRow.getColumnLength()) &&
            (this.getColumnScale() == otherRow.getColumnScale()) );
  }

  // Return true if this records almost match, just doesn't compare column number
  public boolean almostMatchesOtherRow(Db2TableColumn otherRow) {
    return ((this.getColumnName().compareTo(otherRow.getColumnName()) == 0) &&
            (this.getColumnNulls().compareTo(otherRow.getColumnNulls()) == 0) &&
            almostSameColumnType(this, otherRow) &&
            (this.getColumnLength() == otherRow.getColumnLength()) &&
            (this.getColumnScale() == otherRow.getColumnScale()) );
  }
  
  // Returns TRUE if the column types 'almost' match... i.e. some db's want CHAR data
  // defined as GRAPHIC so though they're not the same they are 'almost' the same :)  Did this
  // cause when reporting differences in schema's I didn't want to report this as a 
  // difference.
  public boolean almostSameColumnType(Db2TableColumn col1, Db2TableColumn col2) {
    String colType1 = col1.getColumnType().toUpperCase();
    String colType2 = col2.getColumnType().toUpperCase();
    if (colType1.compareTo(colType2) == 0) return true;  // Same
    
    if ("CHAR GRAPHIC".indexOf(colType1) >= 0) return ("CHAR GRAPHIC".indexOf(colType2) >= 0);
    if ("VARCHAR VARGRAPH".indexOf(colType1) >= 0) return ("VARCHAR VARGRAPH".indexOf(colType2) >= 0);
    return false;
  }
  
  
  
  @Override
  public String toString() {
    return "Db2TableColumn [tableName=" + tableName + ", tableCreator="
        + tableCreator + ", columnName=" + columnName + ", columnNumber="
        + columnNumber + ", columnNulls=" + columnNulls + ", columnType="
        + columnType + ", columnLength=" + columnLength + ", columnScale="
        + columnScale + "]";
  }

  /*
   * Output the column attributes in csv format
   */
  public String toCsvString() {
    return tableName + "," + 
           tableCreator + "," +
           columnName + "," + 
           columnNumber + "," + 
           columnNulls + "," + 
           columnType + "," +  
           columnLength + "," + 
           columnScale;
  }  
}
