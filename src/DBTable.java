
import java.io.File;
import java.util.*;


/**
 * @author lee
 * @create 2021-03-16 14:45
 */
public class DBTable {
    private String name;
    private HashMap<Integer, ArrayList<String>> IndexTable;
    private ArrayList<String> columns;
    private File tableFile;
    private int id;

    public DBTable(File tableFile)
    {
        this.name = tableFile.getName();
        this.tableFile = tableFile;
        this.IndexTable = new HashMap<Integer, ArrayList<String>>();
        this.columns = new ArrayList<String>();
        new DBIO(tableFile,this, true);
        this.id = IndexTable.size() + 1;
    }

    public String getName() {
        return this.name;
    }

    public HashMap<Integer,ArrayList<String>> getIndexTable()
    {
        return IndexTable;
    }

    public void deleting()
    {
        new DBIO(tableFile, true);
    }

    public void addColumns(List<String> cols)
    {
        if (!columns.contains("id")) {
            columns.add("id");
        }
        for (String s: cols) {
            this.columns.add(s);
        }
        for (ArrayList<String> value: IndexTable.values()) {
            for (int i=0; i< cols.size(); i++) {
                value.add("");
            }
        }
        storeTable();
    }

    public void delColumns(List<String> cols) throws Exception
    {
        ArrayList<Integer> delCol = new ArrayList<>();
        for (String s: cols) {
            int i = getSequenceOfColumns(s);
            if (i < 0) {
                throw new Exception();
            }
            delCol.add(i);
        }
        Collections.sort(delCol,Collections.reverseOrder());
        for (int i: delCol) {
            columns.remove(i);
        }
        for (ArrayList<String> value: IndexTable.values()) {
            for (int i : delCol) {
                if (i < value.size()) {
                    value.remove(i);
                }
            }
        }
        storeTable();
    }

    public void addValues(List<String> values)
    {
        ArrayList<String> line = new ArrayList<>();
        int newId = this.id++;
        if(!values.contains("id")) {
            line.add(String.valueOf(newId));
        }
        for (String i:values) {
            line.add(i);
        }
        getIndexTable().put(this.id,line);
        storeTable();
    }

    public int getSequenceOfColumns(String column)
    {
        return columns.indexOf(column);
    }

    public ArrayList<String> getColumns()
    {
        return columns;
    }

    public int getNumberOfColumn ()
    {
        return columns.size();
    }

    public String select(List<String> cols, List<DBCondition> cds, boolean isStar) throws Exception
    {
        String result = "";
        ArrayList<Integer>  indexOfColumns = IndexOfCols(cols,isStar);
        if (isStar) {
            for (String s : columns) {
                result += s + "\t";
            }
        }
        else {
            for (String s : cols) {
                result += s + "\t";
            }
        }
        result += "\n";
        for (ArrayList<String> value: IndexTable.values()) {
            if (!isMeetCDs(value,cds)) {
                continue;
            }
            for (int i: indexOfColumns) {
                if (i<=3)
                    result += value.get(i) + "\t";
            }
            result += "\n";
        }
        return result;
    }

    public ArrayList<Integer> IndexOfCols(List<String> cols, boolean isAll) throws Exception
    {
        ArrayList<Integer>  indexOfColumns = new ArrayList<>();
        if (isAll) {
            for (int i=0; i<columns.size(); i++) {
                indexOfColumns.add(i);
            }
        }
        else {
            for(String s:cols) {
                int index;
                if ((index = getSequenceOfColumns(s)) != -1) {
                    indexOfColumns.add(index);
                }
                else {
                    throw new Exception();
                }
            }
        }
        return indexOfColumns;
    }

    public void updateData(List<String> cols,List<String> vals,List<DBCondition> cds) throws Exception
    {
        ArrayList<Integer>  indexOfColumns = IndexOfCols(cols,false);
        for (ArrayList<String> value: IndexTable.values()) {
            if (!isMeetCDs(value,cds)) {
                continue;
            }
            for (int i=0; i<indexOfColumns.size(); i++) {
                value.set(indexOfColumns.get(i),vals.get(i));
            }
        }
        storeTable();
    }

    public void deleteData(List<DBCondition> cds) throws Exception
    {
        Iterator entries = IndexTable.entrySet().iterator();
        while (entries.hasNext()) {
            Map.Entry entry = (Map.Entry) entries.next();
            Integer key = (Integer)entry.getKey();
            ArrayList<String> value = (ArrayList<String>)entry.getValue();
            if (isMeetCDs(value,cds)) {
                entries.remove();
            }
        }
        storeTable();
    }

    public boolean isMeetCDs (List<String> value, List<DBCondition> cds) throws Exception
    {
        if (cds == null || cds.size() == 0) {
            return true;
        }
        Stack<Boolean> st = new Stack<>();
//        System.out.println(value);
//        for (DBCondition c: cds) {
//            if (c.getType() != null) {
//                System.out.println(c.getType());
//            }
//            else {
//                System.out.println(c.column + c.getOp() + c.getValue());
//            }
//        }
        for (DBCondition c: cds) {
            if (c.getType() != null && c.getType().equals("AND")) {
                boolean flag1 = st.pop();
                boolean flag2 = st.pop();
                st.push(flag1 && flag2);
            }
            else if (c.getType() != null && c.getType().equals("OR")) {
                boolean flag1 = st.pop();
                boolean flag2 = st.pop();
                st.push(flag1 || flag2);
            }
            else {
                st.push(isMeet(value,c));
            }
        }
        return st.pop();
    }

    public boolean isMeet(List<String> value, DBCondition cd) {
        int index = getSequenceOfColumns(cd.getColumn());
        if (cd.getValue() != null) {
            if (cd.getValue().matches("^[0-9.]+$")) {
                double a = Double.parseDouble(cd.value);
                double b = Double.parseDouble(value.get(index));
                switch (cd.getOp()) {
                    case "==":
                        return Math.abs(b - a) < 0.00001;
                    case ">=":
                        return b - a > -0.00001;
                    case "<=":
                        return b - a <= 0.00001;
                    case ">":
                        return b - a > 0;
                    case "<":
                        return b - a < 0;
                    case "!=":
                        return Math.abs(b - a) > 0.00001;
                    case "LIKE":
                        return value.get(index).contains(cd.value);
                }
            } else {
                switch (cd.getOp()) {
                    case "==":
                        return value.get(index).equals(cd.value);
                    case ">=":
                        return value.get(index).compareTo(cd.value) >= 0;
                    case "<=":
                        return value.get(index).compareTo(cd.value) <= 0;
                    case ">":
                        return value.get(index).compareTo(cd.value) > 0;
                    case "<":
                        return value.get(index).compareTo(cd.value) < 0;
                    case "!=":
                        return !value.get(index).equals(cd.value);
                    case "LIKE":
                        return value.get(index).contains(cd.value);
                }
            }
        }
        return false;
    }


    public void storeTable()
    {
       DBIO storeTable = new DBIO(tableFile,this, false);
    }

    public String toString()
    {
        return "columns " + columns.toString() + "\n" +IndexTable.toString();
    }


    public static void main(String[] args)
    {

//        String path ="databases" + File.separator + "contact" + File.separator + "contact-details.tab";
//        File contact = new File(path);
//        try {
//            DBTable test = new DBTable(contact);
//            System.out.println(test.getIndexTable().toString());
//        } catch (Exception e) {
//            e.printStackTrace();
//        }

    }


 }
