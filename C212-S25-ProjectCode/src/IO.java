import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * this is the IO utility class
 */
public class IO {

    /**
     * Reads the table's data from a csv file
     *
     * Implement the following algorithm
     *
     * Open the csv file from the folder (corresponding to the tablename)
     *   For each line in the csv file
     *     Parse the line to get attribute values
     *     Create a new tuple with the schema of the table
     *     Set the tuple values to the attribute values
     *     Add the tuple to the table
     * Close file
     *
     * Return table
     * @param tablename
     * @param schema
     * @param folder
     * @return
     */
    public static ITable readTable(String tablename, ISchema schema, String folder) {
        ITable table = new Table(tablename, schema);
    String path = folder + "/" + tablename + ".csv";

    try (BufferedReader reader = new BufferedReader(new FileReader(path))) {
        String line;
        while ((line = reader.readLine()) != null) {
            if (line.trim().isEmpty()) continue;  

            String[] tokens = line.split(",");
            Object[] values = new Object[tokens.length];

            for (int i = 0; i < tokens.length; i++) {
                String type = schema.getType(i);
                String token = tokens[i].trim();

                switch (type) {
                    case "Integer":
                        values[i] = Integer.parseInt(token);
                        break;
                    case "Double":
                        values[i] = Double.parseDouble(token);
                        break;
                    case "String":
                        values[i] = token;
                        break;
                    default:
                        System.err.println("Unknown type: " + type);
                }
            }

            Tuple tuple = new Tuple(schema);
            tuple.setValues(values);
            table.addTuple(tuple);
        }
    } catch (IOException e) {
        System.err.println("⚠ Error reading table " + tablename + ": " + e.getMessage());
    }

    return table;
}
    /**
     * Writes the tables' data to a csv file
     *
     * Implement the following algorithm
     *
     * Open the csv file from the folder (corresponding to the tablename)
     * Clear all file content
     * For each tuple in table
     *   Write the tuple values to the file in csv format
     *
     * @param table
     * @param folder
     */
    public static void writeTable(ITable table, String folder) {
        String path = folder + "/" + table.getName() + ".csv";
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(path, false))) {
            for (ITuple tuple : table.getTuples()) {
                Object[] values = tuple.getValues();
                for (int i = 0; i < values.length; i++) {
                    writer.write(values[i].toString());
                    if (i < values.length - 1) {
                        writer.write(","); 
                    }
                }
                writer.newLine();  
            }
        } catch (IOException e) {
            System.err.println("Error writing table " + table.getName() + ": " + e.getMessage());
        }
    }

    /**
     * Prints the table to console (mainly used to print the output of the select query)
     *
     * Implements the following algorithm
     *
     * Print the attribute names from the schema as tab separated values
     * For each tuple in the table
     *   Print the values in tab separated format
     *
     *
     * @param table
     * @param schema
     */
    public static void printTable(ITable table, ISchema schema) {
        int size = schema.getAttributes().size();
        for (int i = 0; i < size; i++) {
            System.out.print(schema.getName(i));
            if (i < size - 1) System.out.print("\t");
        }
        System.out.println();
    
        for (ITuple tuple : table.getTuples()) {
            Object[] values = tuple.getValues();
            for (int i = 0; i < values.length; i++) {
                System.out.print(values[i]);
                if (i < values.length - 1) System.out.print("\t");
            }
            System.out.println();  
        }

    }


    /**
     * Writes a tuple to a csv file
     *
     * Implements the following algorithm
     *
     * Open the csv file from the folder (corresponding to the tablename)
     * Append the tuple (as array of strings) in the csv format to the file
     *
     * @param tableName
     * @param values
     * @param folder
     */
    public static void writeTuple(String tableName, Object[] values, String folder) {
        String path = folder + "/" + tableName + ".csv";

        try (BufferedWriter writer = new BufferedWriter(new FileWriter(path, true))) {
            for (int i = 0; i < values.length; i++) {
                writer.write(values[i].toString());
                if (i < values.length - 1) {
                    writer.write(","); 
                }
            }
            writer.newLine();  
        } catch (IOException e) {
            System.err.println("Error appending to table " + tableName + ": " + e.getMessage());
        }
    }

    /**
     * Reads and parses the schema, creates schema objects and (empty) tables and adds them to the provided database
     * The schema is stored in a text file:
     *
     * Implements the following algorithm
     *
     * Open the schema file
     * For each line
     *   Parse the line to get the table name, attribute names and attribute types
     *   Create an attribute map of (index, att_name:att_type) pairs
     *   For each attribute
     *     Store the index and name:type pair in the map (index represents the position of attribute in the schema)
     *   Create a new schema object with this attribute map
     *   Add the schema object to the database
     *   Create a new table object with the table name and the schema object
     *   Add the table to the database
     *
     * @param schemaFileName
     * @param folderName
     * @param db
     */
    public static void readSchema(String schemaFileName, String folderName, Database db) {
        String path = folderName + "/" + schemaFileName;
        try (BufferedReader reader = new BufferedReader(new FileReader(path))) {
            String line;
            while ((line = reader.readLine()) != null) {
                int openParen = line.indexOf("(");
                int closeParen = line.indexOf(")");
    
                String tableName = line.substring(0, openParen).trim();
                String attributesRaw = line.substring(openParen + 1, closeParen);
                String[] attrDefs = attributesRaw.split(",");
    
                Map<Integer, String> attrMap = new HashMap<>();
                for (int i = 0; i < attrDefs.length; i++) {
                    attrMap.put(i, attrDefs[i].trim());
                }
    
                ISchema schema = new Schema(attrMap);
                db.addSchema(schema);
                ITable table = new Table(tableName, schema);
                db.addTable(table);
            }
        } catch (IOException e) {
            System.err.println("Error: " + e.getMessage());
        }
    }

}
