import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * The main database class
 * Database as a list of tables, list of schemas and a folder name where the database is stored
 * Database is stored (on the disk) in the form of three csv files and schema text file
 */
class Database {
    private List<ITable> tables;
    private List<ISchema> schemas;
    private String folderName;

    /**
     * Constructor
     * Creates the empty tables and schema lists
     * Reads the schema file to add schemas to the database
     * Populates the database table (with the data read from the csv files)
     * @param folderName
     * @param schemaFileName
     */
    public Database(String folderName, String schemaFileName) {
        this.schemas = new ArrayList<>();
            this.tables = new ArrayList<>();
            this.folderName = folderName;
            IO.readSchema(schemaFileName, folderName, this); 
            populateDB();
        }

    /**
     * Adds a table to the database
     * @param table
     */
    public void addTable(ITable table) {
        tables.add(table);
    }

    /**
     * Adds a table schema to the database
     * @param schema
     */
    public void addSchema(ISchema schema) {
        schemas.add(schema);
    }

    /**
     * Return the list of tables in the database
     * @return
     */
    public List<ITable> getTables() {
        return tables;

    }

    /**
     * Returns the list of schemas in the database
     * @return
     */
    public List<ISchema> getSchemas() {
        return schemas;

    }

    /**
     * The list of tables in the database is initialized with empty tables in the constructor
     * An empty table has a name and an empty list of tuples
     * This method sets the empty table in the list to the one provided as a parameter
     * @param table
     */
    public void updateTable(ITable table) {
        for (int i = 0; i < tables.size(); i++) {
            if (tables.get(i).getName().equals(table.getName())) {
                tables.set(i, table);
            }
                
        }
    }

    /**
     * Populates the database
     *
     * Implements the following algorithm
     *
     * For each table in the db (tables are initially empty)
     *   Get the table's data from the csv file (by calling the read table method)
     *   Update the table (by calling the udpate table method)
     */
    public void populateDB() {
        for (ITable table : tables){
            String name = table.getName();
            ISchema schema = table.getSchema();
            ITable fullTable = IO.readTable(name, schema, folderName);
            updateTable(fullTable);
        }
        
    }
    /**
     * Insert data into a table based upon the insert query
     * If the query is invalid throws an InvalidQueryException
     *
     * Implements the following algorithm
     *
     * Parse the insert into clause to get the table name, attribute name(s) and value(s)
     * If the query in not valid
     *   Throw an invalid query exception
     *   Exit
     * Create a new tuple with the schema of the table
     * Set the tuple values to the values from the query
     * Open the file corresponding to the table name
     * Append the tuple values (as comma separated values) to the end of the file
     *
     * @param query
     * @throws InvalidQueryException
     */
    public void insertData(String query) throws InvalidQueryException {
        if (!query.startsWith("INSERT INTO") || !query.contains("VALUES")) {
            throw new InvalidQueryException("Query must contain 'INSERT INTO' and 'VALUES'");
        }
   
        String[] parts = query.split("VALUES"); 
        String left = parts[0].trim();
        String right = parts[1].trim();
        String removeinsertInto = left.replace("INSERT INTO", "").trim();
        int parenStart = removeinsertInto.indexOf("(");
        String tableName = removeinsertInto.substring(0, parenStart).trim();
        String attrpart = removeinsertInto.substring(parenStart + 1, removeinsertInto.indexOf(")")).trim();
        String[] attributes = attrpart.split(",");
        String valueString = right.substring(right.indexOf("(") + 1, right.indexOf(")")).trim();
        String[] values = valueString.split(",");
        if (attributes.length != values.length) {
            throw new InvalidQueryException("Number of attributes and values must match");
        }


        ITable table = null; 
        for (ITable t : tables) {
            if (t.getName().equals(tableName)) {
                table = t;
                break;
            }
        }
        if (table == null) { 
            throw new InvalidQueryException("Table '" + tableName + "' does not exist");
        }


        ITuple tuple = new Tuple(table.getSchema());
        for (int i = 0; i < attributes.length; i++) { 
            String attr = attributes[i].trim();
            String val = values[i].trim().replaceAll("'", "");
        
            for (int j = 0; j < table.getSchema().getAttributes().size(); j++) {
                String schemaAttr = table.getSchema().getName(j);
                if (schemaAttr.equals(attr)) {
                    String type = table.getSchema().getType(j);
                    Object convertedValue;
        
                    switch (type) {
                        case "Integer":
                            convertedValue = Integer.parseInt(val);
                            break;
                        case "Double":
                            convertedValue = Double.parseDouble(val);
                            break;
                        case "String":
                            convertedValue = val;
                            break;
                        default:
                            throw new InvalidQueryException("Unknown type: " + type);
                    }
        
                    tuple.setValue(j, convertedValue);
                    break;
                }
            }
        }
        table.addTuple(tuple);
        IO.writeTable(table, folderName);  
        updateTable(IO.readTable(table.getName(), table.getSchema(), folderName));
    }

    /**
     * Selects data from a table (and returns it in the form of a results table)
     * If the query in not valid, throws an InvalidQueryException
     *
     * A query is valid if
     *
     * 1.	It has a select clause (select keyword followed by at least one attribute name)
     * 2.	It has a from clause (from keyword followed by a table name)
     * 3.	All the attribute names in the select clause are in the schema
     * 4.	The table name in the from clause is in the schema
     * 5.	All the attribute names in the where clause (if present) are in the schema
     * 6.	The attribute name in the order by clause (if present) is in the schema
     *
     * Implements the following algorithm
     *
     * Parse the query to get the select, from, where and order by clauses and the attribute and table names and condition
     * If the query is not valid
     *   Throw an invalid query exception
     *   Exit
     * Create a new results schema based with the attributes from the select clause
     * Create a new result table
     * For each tuple in the table
     *   If the tuple matches the where clause condition(s)
     *     Create a new results tuple using the result schema
     *     Set the results tuple values to the current tuple corresponding values
     *     Add the results tuple to the result table
     * Return results table
     *
     *
     * @param query
     * @return
     * @throws InvalidQueryException
     */
    public ITable selectData(String query) throws InvalidQueryException {
        if (!query.startsWith("SELECT") || !query.contains("FROM")) {
            throw new InvalidQueryException("Query must contain SELECT and FROM clauses.");
        }
        
        String[] selectSplit = query.split("FROM");  
        String selectPart = selectSplit[0].replace("SELECT", "").trim();
        String fromPart = selectSplit[1].trim();

        String tableName;
        String whereClause = null;

        if (fromPart.contains("WHERE")) {
            String[] fromSplit = fromPart.split("WHERE");
            tableName = fromSplit[0].trim();          
            whereClause = fromSplit[1].trim();         
        } else {
            tableName = fromPart.trim();              
        }
        ITable table = null;

        for (ITable t : tables) {
            if (t.getName().equals(tableName)) {
                table = t;
                break;
            }
        }

        if (table == null) {
            throw new InvalidQueryException("Table '" + tableName + "' does not exist.");
        }
        String[] selectedAttrs = selectPart.split(",");
        for (int i = 0; i < selectedAttrs.length; i++) {
            selectedAttrs[i] = selectedAttrs[i].trim(); 
        }

        Map<Integer, String> schemaAttributes = table.getSchema().getAttributes();
        List<String> schemaNames = new ArrayList<>();

        for (int i = 0; i < schemaAttributes.size(); i++) {
            schemaNames.add(table.getSchema().getName(i));
        }

        for (String attr : selectedAttrs) {
            if (!schemaNames.contains(attr)) {
                throw new InvalidQueryException("Attribute '" + attr + "' not in table '" + tableName + "'");
            }
        }

        Map<Integer, String> resultAttributes = new HashMap<>();
        ISchema originalSchema = table.getSchema();

        int resultIndex = 0;
        for (int i = 0; i < originalSchema.getAttributes().size(); i++) {
            String attrName = originalSchema.getName(i);
            String attrType = originalSchema.getType(i);

            if (Arrays.asList(selectedAttrs).contains(attrName)) {
                resultAttributes.put(resultIndex, attrName + ":" + attrType);
                resultIndex++;
            }
        }

        ISchema resultSchema = new Schema(resultAttributes);
        ITable resultTable = new Table("result", resultSchema);

        for (ITuple tuple : table.getTuples()) {
            if (whereClause != null) {
                String[] conditionParts = whereClause.split("=");
                String condAttr = conditionParts[0].trim();
                String condValue = conditionParts[1].trim();
        
                boolean match = false;
                for (int i = 0; i < table.getSchema().getAttributes().size(); i++) {
                    if (table.getSchema().getName(i).equals(condAttr)) {
                        Object val = tuple.getValue(i);
                        if (val.toString().equals(condValue)) {
                            match = true;
                        }
                        break;
                    }
                }
                if (!match) {
                    continue; 
                }
            }
        
            ITuple resultTuple = new Tuple(resultSchema);
        
            for (int i = 0; i < selectedAttrs.length; i++) {
                String selectedAttr = selectedAttrs[i];
        
                for (int j = 0; j < table.getSchema().getAttributes().size(); j++) {
                    if (table.getSchema().getName(j).equals(selectedAttr)) {
                        resultTuple.setValue(i, tuple.getValue(j));
                        break;
                    }
                }
            }
        
            resultTable.addTuple(resultTuple);
        }
        return resultTable;
    }

    /**
     * Delete data from a table
     * If the query in not valid, throws an InvalidQueryException
     *
     * Implements the following algorithm
     *
     * Parse the query to get the from and where clauses
     * Parse the from clause to get the table name
     * If the query in not valid
     *   Throw an invalid query exception
     *   Exit
     * If where clause is not empty
     *   Parse the where clause to get the the condition
     *   For each tuple in the table
     *     If the where clause condition is true
     *       Remove the tuple from the table
     * Else
     *   For each tuple in the table
     *     Remove the tuple from the table
     * Write the table to the file
     *
     * @param query
     * @throws InvalidQueryException
     */
    public void deleteData(String query) throws InvalidQueryException {
        if (!query.startsWith("DELETE FROM")) {
            throw new InvalidQueryException("DELETE query must start with 'DELETE FROM'");
        }
        
        String tableName;
        String whereClause = null;
        
        String cleaned = query.replace("DELETE FROM", "").trim();
        
        if (cleaned.contains("WHERE")) {
            String[] parts = cleaned.split("WHERE");
            tableName = parts[0].trim();
            whereClause = parts[1].trim(); 
        } else {
            tableName = cleaned;
        }
        ITable table = null;

        for (ITable t : tables) {
            if (t.getName().equals(tableName)) {
                table = t;
                break;
            }
        }

        if (table == null) {
            throw new InvalidQueryException("Table '" + tableName + "' does not exist.");
        }
        List<ITuple> toRemove = new ArrayList<>();

        if (whereClause != null) {
            String[] conditionParts = whereClause.split("=");
            String condAttr = conditionParts[0].trim();
            String condValue = conditionParts[1].trim();

            for (ITuple tuple : table.getTuples()) {
                for (int i = 0; i < table.getSchema().getAttributes().size(); i++) {
                    if (table.getSchema().getName(i).equals(condAttr)) {
                        Object val = tuple.getValue(i);
                        if (val.toString().equals(condValue)) {
                            toRemove.add(tuple);
                        }
                        break;
                    }
                }
            }
        } else {
            toRemove.addAll(table.getTuples());
        }

        for (ITuple t : toRemove) {
            table.getTuples().remove(t);
        }

        IO.writeTable(table, folderName);

        ITable refreshedTable = IO.readTable(table.getName(), table.getSchema(), folderName);
        updateTable(refreshedTable);    

    }

}
