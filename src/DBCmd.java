import javax.naming.ldap.Control;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.locks.Condition;

/**
 * @author lee
 * @create 2021-03-17 21:49
 */

//    DBCmd is super class for many CMDs, have some useful methods
public abstract class DBCmd {
    List<DBCondition> conditions;
    List<String> colNames;
    List<String> tableNames;
    String DBName;
    String commandType;

    public DBCmd(){}

    public abstract String query(DBController controller);

    public void setDBName(String DBName) {
        this.DBName = DBName;
    }

    public void addColNames(String colName)
    {
        if (colNames == null) {
            colNames = new ArrayList<String>();
        }
       colNames.add(colName);
    }

    public void addTableName(String tableName) {
        if (this.tableNames == null) {
            tableNames = new ArrayList<>();
        }
        tableNames.add(tableName);
    }

    public void addConditions(DBCondition cd) {
        if (this.conditions == null) {
            conditions = new ArrayList<>();
        }
        conditions.add(cd);
    }
}

class UseCMD extends DBCmd {

    public UseCMD()
    {
        this.commandType = "use";
    }

//    use message stored in DBCmd class to set database
    public String query(DBController controller)
    {
        try {
            controller.setCurrentDB(DBName);
        } catch (Exception e) {
            System.err.println("[ERROR]: Unknown database ");
            return "[ERROR]: Unknown database ";
        }
        return "[OK]";
    }

}

class CreateCMD extends DBCmd {

        public CreateCMD()
    {
        commandType = "create";
    }

    //    use message stored in CreatCmd class to creat table or database
    public String query(DBController controller) {
        if (DBName != null) {
                try {
                    controller.addNewDataBase(DBName);
                } catch (Exception e) {
                    System.out.println("[ERROR]: Database already existed");
                    return "[ERROR]: Database already existed";
                }
            } else {
                try {
                    controller.getCurrentDB().addNewTable(tableNames.get(0));
                    if (colNames != null) {
                        controller.addColumns(tableNames.get(0), colNames);
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                    System.out.println("[ERROR]: Table already existed or do not use database");
                    return "[ERROR]: Table already existed or do not use database";
                }
            }
        return "[OK]";
    }
    public static void main(String[] args) {
    }
}

//    use message stored in DropCmd class to creat table or database
class DropCMD extends DBCmd {

    public DropCMD()
    {
        this.commandType = "drop";
    }

    public String query(DBController controller) {
        if (DBName != null) {
            try {
                controller.deleteDB(DBName);
            } catch (Exception e) {
                System.out.println("[ERROR]: No database");
                return "[ERROR]: No database";
            }
        }
        else {
            try {
                controller.getCurrentDB().deleteTable(tableNames.get(0));
            } catch (Exception e) {
                System.out.println("[ERROR]: No table");
                return "[ERROR]: No table";
            }
        }
        return "[OK]";
    }
}

//    use message stored in AlterCmd class to alter column in table
class AlterCMD extends DBCmd {
    boolean isAdd;

    public AlterCMD()
    {
        this.commandType = "alter";
    }

    public String query(DBController controller) {
        if (tableNames == null || colNames == null) {
            try {
                throw  new Exception();
            } catch (Exception e) {
                System.out.println("[ERROR]: No table");
                return "[ERROR]: No table";
            }
        }
        if (isAdd == true) {
            try {
                controller.addColumns(tableNames.get(0), colNames);
            } catch (Exception e) {
                System.err.println("[ERROR]: No database");
                return "[ERROR]: No database";
            }
        } else {
            try {
                controller.deleteColumns(tableNames.get(0), colNames);
            } catch (Exception e) {
                System.err.println("[ERROR]: No colNames");
                return "[ERROR]: No colNames";
            }
        }
        return "[OK]";
    }
}

//    use message stored in InsertCmd class to insert data in table
class InsertCMD extends DBCmd {

    public InsertCMD()
    {
        this.commandType = "insert";
    }

    public String query(DBController controller) {
       try {
           if (tableNames == null || colNames == null) {
               throw new Exception();
           }
           controller.addValues(tableNames.get(0), colNames);
       }
       catch (Exception e) {
            System.err.println("[ERROR]: No table");
            return "[ERROR]: No table";
        }
        return "[OK]";
    }
}

//  DBCondition class contains required member variables to represent condition sentence
class DBCondition {
    String column;
    String op;
    String value;
    String type;

    public DBCondition() {
    }

    public void setColumn(String columnName) {
        this.column = columnName;
    }

    public void setOp(String op)
    {
        this.op = op;
    }

    public void setValue(String value)
    {
        this.value = value;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getColumn() {
        return column;
    }

    public String getOp() {
        return op;
    }

    public String getValue() {
        return value;
    }

    public String getType() {
        return type;
    }
}

//    use message stored in SelectCmd class to select required data in table
class SelectCMD extends DBCmd {
    boolean isStar = false;

    public SelectCMD() {
        this.commandType = "select";
    }

    public void setStar() {
        isStar = true;
    }


    public String query(DBController controller) {
        String result = "";
            try {
                result = controller.selectTables(tableNames.get(0),colNames, conditions,isStar);
            } catch (Exception e) {
                System.err.println("[ERROR]: fail to select");
                return "[ERROR]: fail to select";
            }
        return "[OK]" + "\n" + result;
    }
}

class UpdateCMD extends DBCmd {
    ArrayList<String> values;

    public ArrayList<String> getValues() {
        return values;
    }

    public void  addValue(String value)
    {
        if (getValues() == null) {
            values = new ArrayList<String>();
        }
        values.add(value);
    }

    //    use message stored in UpdateCmd class to update specific data in table
    public String query(DBController controller) {
        try {
            controller.getCurrentDB().getTable(tableNames.get(0)).updateData(colNames,values,conditions);
        } catch (Exception e) {
            System.err.println("[ERROR]: Fail to update");
            return "[ERROR]: Fail to update";
        }
        return "[OK]";
    }
}

//    use message stored in CreatCmd class to update specific data in table
class DeleteCMD extends DBCmd {

    public String query(DBController controller) {
        try {
            controller.getCurrentDB().getTable(tableNames.get(0)).deleteData(conditions);
        } catch (Exception e) {
            System.err.println("[ERROR]: Fail to delete");
            return "[ERROR]: Fail to delete";
        }
        return "[OK]";
    }
}

//    use message stored in JoinCmd class to join two table in specific attributes
class JoinCMD extends DBCmd {
    int separator;

    public void setSeparator(int separator) {
        this.separator = separator;
    }

    public int getSizeOfColumn() {
        return colNames.size();
    }

    public String query(DBController controller) {
        try {
            String result;
            result = controller.getCurrentDB().joinTables(tableNames, colNames,separator);
            return "[OK]" + "\n" + result;
        } catch (Exception e) {
            e.printStackTrace();
            System.err.println("[ERROR]: Fail to query");
            return "[ERROR]: Fail to query";
        }
    }
}

