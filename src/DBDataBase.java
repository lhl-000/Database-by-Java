import jdk.jfr.StackTrace;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * @author lee
 * @create 2021-03-16 21:11
 */
//  database class contains corresponding tables , in this class can add, delete, join tables and build, store database
public class DBDataBase {

    private String path;
    private File databaseFile;
    private File[] tableFiles;
    private ArrayList<DBTable> tables;


    public DBDataBase(String nameOfDataBase)
    {
        this.path = "databases" + File.separator + nameOfDataBase;
        this.databaseFile = new File(path);
        tables = new ArrayList<DBTable>();
        if (!databaseFile.exists()) {
            databaseFile.mkdir();
            return;
        }
        this.tableFiles = databaseFile.listFiles();
        loadingTables(tableFiles);
    }

    private void loadingTables (File[] tableFiles)
    {
        for (int i=0; i<tableFiles.length; i++) {
            tables.add(new DBTable(tableFiles[i]));
        }
    }


    public void addNewTable (String tableName) throws Exception
    {
        String fileName = tableName + ".tab";
        if( tables.size() > 0) {
            for(DBTable t : tables) {
                if (t.getName().equals(fileName)) {
                    throw new Exception();
                }
            }
        }
        String newTablePath = this.path + File.separator + fileName;
        File newTableFile = new File(newTablePath);
        tables.add(new DBTable(newTableFile));
    }

    public void deleteTable (String tableName) throws Exception
    {
        int index = getIndexOfTable(tableName);
        if (index >= 0) {
            tables.get(index).deleting();
            tables.remove(index);
        }
        else {
            throw  new Exception();
        }
    }

    public String joinTables(List<String> tableNames, List<String> cols, int separator) throws Exception
    {
        DBTable t1 = getTable(tableNames.get(0));
        DBTable t2 = getTable(tableNames.get(1));
        String result = "";
        ArrayList<String> cols1 = new ArrayList<>();
        ArrayList<String> cols2 = new ArrayList<>();
        for (int i=0; i<=separator; i++) {
            cols1.add(cols.get(i));
        }
        for (int i=separator+1; i<cols.size(); i++) {
            cols2.add(cols.get(i));
        }
        System.out.println(cols2);
        ArrayList<Integer>  indexOfT1Col= t1.IndexOfCols(cols1,false);
        ArrayList<Integer>  indexOfT2Col= t2.IndexOfCols(cols2,false);
        for (String s : cols) {
            result += s + "\t";
        }
        for (String s : t1.getColumns()) {
            if (cols.contains(s)) {
                continue;
            }
            result += s + "\t";
        }
        for (String s : t2.getColumns()) {
            if (cols.contains(s)) {
                continue;
            }
            result += s + "\t";
        }
        result += "\n";
        for (ArrayList<String> value1: t1.getIndexTable().values()) {
            for (ArrayList<String> value2: t2.getIndexTable().values()) {
                boolean flag = true;
                for (int i=0; i<indexOfT1Col.size(); i++) {
                    if (!value1.get(indexOfT1Col.get(i)).equals(value2.get(indexOfT2Col.get(i)))) {
                        flag = false;
                        break;
                    }
                }
                if (flag) {
                    for (int j=0; j<indexOfT1Col.size(); j++) {
                        result += value1.get(indexOfT1Col.get(j)) + "\t";
                    }
                    for (int j=0; j<indexOfT2Col.size(); j++) {
                        result += value2.get(indexOfT2Col.get(j)) + "\t";
                   }
                    for (int j=0; j<value1.size(); j++) {
                        if (!indexOfT1Col.contains(j)) {
                                result += value1.get(j) + "\t";
                        }
                    }
                    for (int j=0; j<value2.size(); j++) {
                        if (!indexOfT2Col.contains(j)) {
                                result += value2.get(j) + "\t";
                        }
                    }
                    result += "\n";
                    break;
                    }
            }
        }
        return result;
    }

    public DBTable getTable (String tableName) throws Exception
    {
        return this.getTables().get(getIndexOfTable(tableName));
    }

    public int getIndexOfTable (String tableName) throws Exception
    {
        for (int i=0; i<tables.size(); i++) {
            if (tables.get(i).getName().equals(tableName + ".tab")) {
                return i;
            }
        }
        throw new Exception();
    }

    public ArrayList<DBTable> getTables()
    {
        return tables;
    }

    public int getNumberOfTables()
    {
        return tables.size();
    }

    public void storeDataBase()
    {
        for (int i=0; i<getNumberOfTables(); i++) {
            tables.get(i).storeTable();
        }
    }

    public String getName()
    {
        return databaseFile.getName();
    }

    public String getPath()
    {
        return path;
    }

    public File getDatabase()
    {
        return databaseFile;
    }

    public File[] getTableFiles()
    {
        return tableFiles;
    }

    public static void main(String[] args)
    {
            DBDataBase test = new DBDataBase("contact");
            for (int i=0; i< test.getNumberOfTables(); i++) {
                System.out.println(test.getTables().get(i));
            }
//            try {
//                test.addNewTable("test");
//            } catch (Exception e) {
//                System.err.println("Table has already existed");
//            }
//            test.deleteTable("test");
            System.out.println(test.tables);
            test.storeDataBase();
    }

}


