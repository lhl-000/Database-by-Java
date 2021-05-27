
import java.io.*;
import java.util.ArrayList;

/**
 * @author lee
 * @create 2021-03-16 10:13
 */
// IO class can read and write table file in specific format
public class DBIO {

    private File tableFile;
    private DBTable table;


    public DBIO(File tableFile, DBTable table, boolean loading)
    {
        this.tableFile = tableFile;
        this.table = table;
        if (!tableFile.exists()) {
            try {
                tableFile.createNewFile();
            } catch (IOException e) {
                System.err.println("Failed to create table file");
            }
        }
        if (loading == true) {
            loadToIndexTable();
        }
        else {
            storeFromIndexTable();
        }

//        if (loading == true && !tableFile.exists()) {
//            throw new FileNotFoundException();
//        }
//        else if (loading == false && !tableFile.exists()) {
//            tableFile.createNewFile();
//            storeFromIndexTable();
//        }
//        else (loading == false && tableFile.exists()) {
//            tableFile.createNewFile();
//            storeFromIndexTable();
//        }
//        else {
//            loadToIndexTable();
//        }
    }

    public DBIO(File tableFile, boolean deleting)
    {
        if (deleting == true) {
            tableFile.delete();
        }
    }

    public void loadToIndexTable(){
        try {
            BufferedReader buffReader = new BufferedReader(new FileReader(tableFile));
            String line = buffReader.readLine();
            //read column
            if (line != null) {
                loadColumn(line);
            }
            //read data to index table
            while ((line = buffReader.readLine()) != null) {
                loadIndexTable(line);
            }
            buffReader.close();
        }
        catch (IOException e) {
            System.err.println("Failed to load file : " + tableFile.getName());
        }
    }


    public void loadIndexTable (String line)
    {
        //read data for index tables
        ArrayList<String> lineTable = new ArrayList<String>();
        String[] lineSplit = line.split("\t");
        for (String s:lineSplit) {
            lineTable.add(s);
        }
        int id = Integer.valueOf(lineSplit[0]);
        table.getIndexTable().put(id,lineTable);
    }

    public void loadColumn (String line)
    {

        String[] columnsTable = line.split("\t");
        for (String s:columnsTable) {
            table.getColumns().add(s);
        }
    }

    public void storeFromIndexTable()
    {
        try{
            BufferedWriter bufferWriter = new BufferedWriter(new FileWriter(tableFile));
            if (table.getColumns().size() != 0) {
                storeColumn(bufferWriter);
            }
            if (table.getIndexTable().size() != 0) {
                storeData(bufferWriter);
            }
            bufferWriter.close();
        }
        catch (IOException e) {
            System.err.println("Failed to store table" + tableFile);
        }

    }

    public void storeColumn (BufferedWriter bufferWriter) throws IOException
    {
        String line = toRightFormat(table.getColumns());
        bufferWriter.write(line);
        bufferWriter.newLine();
    }

    public void storeData (BufferedWriter bufferWriter) throws IOException
    {
        for (ArrayList value: table.getIndexTable().values()) {
            String line = toRightFormat(value);
            bufferWriter.write(line);
            bufferWriter.newLine();
        }

    }

    public String toRightFormat (ArrayList<String> strArr)
    {
        String line = "";
        for (String s:strArr) {
            line += s + '\t';
        }
        return line.substring(0,line.length()-1);
    }

}
