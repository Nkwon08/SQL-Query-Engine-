public class Main {

    public static void main(String[] args) {
        Database db = new Database("db", "schema.txt");
        runQuery("INSERT INTO student (sid, sname, major, byear) VALUES (s100, Alice, CS, 2001)", db);
        runQuery("INSERT INTO student (sid, sname, major, byear) VALUES (s101, Bob, Math, 2002)", db);
        runQuery("INSERT INTO student (sid, sname, major, byear) VALUES (s102, Carol, CS, 2003)", db);

        System.out.println("▶ CS Majors:");
        runQuery("SELECT sid, sname FROM student WHERE major = CS", db);

        runQuery("DELETE FROM student WHERE sid = s100", db);

        System.out.println("▶ All Students After Deletion:");
        runQuery("SELECT sid, sname FROM student", db);
}

    /**
     * Runs the given query on the database
     *
     * Implements the following algorithm
     *
     * Determine the type of query (from select, insert or delete)
     * If select query
     *   Select data
     *   Print results
     * Else if insert query
     *   Insert data
     * Else if delete is given
     *   Delete data
     *
     * @param query
     * @param db
     */
    public static void runQuery(String query, Database db) {
        try {
            String normalized = query.trim().toUpperCase();
            if (normalized.startsWith("SELECT")) {
                ITable result = db.selectData(query);
                IO.printTable(result, result.getSchema());
            } else if (query.startsWith("INSERT INTO")) {
                db.insertData(query);
                System.out.println("Insert successful.");
            } else if (query.startsWith("DELETE FROM")) {
                db.deleteData(query);
                System.out.println("Delete successful.");
            } else {
                System.out.println("Unknown query type.");
            }
        } catch (InvalidQueryException e) {
            System.err.println("Query Error: " + e.getMessage());
        }
    }
}

