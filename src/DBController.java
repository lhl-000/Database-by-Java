import java.io.File;
import java.util.ArrayList;
import java.util.List;

/**
 * @author lee
 * @create 2021-03-16 10:32
 */

// Controller can control different databases, tokenizer and all cmd, it is central class to connect different classes.
// database are represented by folder stored in databases folder
public class DBController {
    private String path = "databases";
    private File[] databaseFiles;
    private ArrayList<DBDataBase> dbDataBases;
    private DBTokenizer tokenizer;
    private DBDataBase currentDB;

    // build controller , tokenizer and database
    public DBController()
    {
        File root = new File(path);
        databaseFiles = root.listFiles();
        dbDataBases = new ArrayList<DBDataBase>();
        this.tokenizer = new DBTokenizer();
        for(File f: databaseFiles) {
            dbDataBases.add(new DBDataBase(f.getName()));
        }
    }

    // parser input statement and return feedback
    public String processing(String statement)
    {
        tokenizer.tokenizing(statement);
        System.out.println(statement);
        DBParser p = new DBParser(this, tokenizer);
        p.parsing();
        return p.getFeedback();
    }


    public DBDataBase getCurrentDB() throws Exception
    {
        if (currentDB == null) {
            throw new Exception();
        }
        return currentDB;
    }

    public void setCurrentDB(String name) throws Exception
    {
       if((currentDB = getDataBase(name)) == null) {
               throw new Exception();
       }
    }

    public void addNewDataBase(String name) throws  Exception
    {
        if (getDataBase(name) != null) {
            throw new Exception();
        }
        dbDataBases.add(new DBDataBase(name));
    }

    public DBDataBase getDataBase(String name)
    {
        for (DBDataBase db: dbDataBases) {
            if (db.getName().equals(name)) {
                return db;
            }
        }
        return null;
    }

    public String selectTables(String tableName, List<String> cols, List<DBCondition> cds, boolean isStar) throws Exception
    {
       return currentDB.getTable(tableName).select(cols,cds,isStar);
    }

    public void addColumns(String tableName, List<String> columns) throws Exception
    {
        currentDB.getTable(tableName).addColumns(columns);
    }


    public void deleteColumns(String tableName, List<String> columns) throws Exception
    {
        currentDB.getTable(tableName).delColumns(columns);
    }

    public void deleteDB(String DBName) throws Exception
    {
        if (getDataBase(DBName) == null) {
            throw new Exception();
        }
        String DBPath = path + File.separator + DBName;
        File DB = new File(DBPath);
        File[] DBTables = DB.listFiles();
        for (File f: DBTables) {
            f.delete();
        }
        DB.delete();
        dbDataBases.remove(getDataBase(DBName));
    }

    public void addValues(String tableName, List<String> values) throws Exception
    {
        getCurrentDB().getTable(tableName).addValues(values);
    }

//    public DBController(String statement)
//    {
//        DBTokenizer tokenizer = null;
//        try {
//            tokenizer = new DBTokenizer(statement);
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
//        DBParser parser= new DBParser(tokenizer.getTokens());
//
//    }

    public static void main(String[] args) {
        DBController c = new DBController();
//      c.processing("DROP DATABASE imdb;");
//        System.out.println(c.processing("CREATE DATABASE imdb;"));
//       c.processing("CREATE DATABASE markbook;");
        System.out.println(c.processing("use imdb;"));
        //c.processing("INSERT INTO movies VALUES ('Mickey Blue Eyes', 'Comedy');");
        System.out.println(c.processing("JOIN movies AND roles ON id AND movie_id;"));
//        c.processing("DROP TABLE actors;");
//        c.processing("CREATE TABLE actors (name, nationality, awards);");
//
//       c.processing("INSERT INTO actors VALUES ('Hugh Grant', 'British', 3);");
//        c.processing("INSERT INTO actors VALUES ('Toni Collette', 'Australian', 12);");
//       c.processing("INSERT INTO actors VALUES ('James Caan', 'American', 8);");
//       c.processing("INSERT INTO actors VALUES ('Emma Thompson', 'British', 10);");
//       System.out.println(c.processing("SELECT * FROM actors WHERE awards >= 10;"));
//        System.out.println(c.processing("JOIN actors AND roles ON id AND actor_id;"));
//        c.processing("INSERT INTO marks VALUES ('Bob', 35, false);");
//        c.processing("INSERT INTO marks VALUES ('Clive', 20, false);");
//       c.processing("UPDATE marks SET mark = 38 WHERE name == 'Clive';");
//        c.processing("SELECT * FROM marks WHERE name == 'Clive';");
//        c.processing("DELETE FROM marks WHERE name == 'Dave';");
//        c.processing("DELETE FROM marks WHERE mark < 40;");
//        c.processing("create table test (a1, a2, a3);");
//        c.processing("insert into table test values (m1, m2, m3);");
//        c.processing("ALTER TABLE test drop a3, m4, m5;");
//        c.processing("select  Name, Age from contact-details where (id = 1) or ((id = 2) and (id = 3));");
//        c.processing("use contact;");
//        c.processing("drop DATABASE test;");
//        c.processing("create TABLE test;");
//        c.processing("drop TABLE test;");
//       c.processing("create TABLE test (1 , 2 , 3) ;");
//        USE imdb;
//        DROP TABLE actors;
//        DROP TABLE movies;
//        DROP TABLE roles;
//        DROP DATABASE imdb;
//        CREATE DATABASE imdb;
//        USE imdb;
//        CREATE TABLE actors (name, nationality, awards);

    }
}
