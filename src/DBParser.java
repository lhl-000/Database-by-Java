import java.util.*;
import java.util.concurrent.locks.Condition;

/**
 * @author lee
 * @create 2021-03-17 10:42
 */
// parser can analyze tokens according to BNF rules, it can control the sequence of token and call different cmd class
public class DBParser {
    DBController controller;
    DBTokenizer tokenizer;
    String feedback ;


    public DBParser(DBController controller, DBTokenizer tokenizer)
    {
        this.controller = controller;
        this.tokenizer = tokenizer;
    }

    // check the first token according command type
    public void parsing()
    {
        if (getToken() != null) {
            if (tokenType().equals("CT")) {
                passToCmd(tokenWord());
            }
            else {
                feedback = "[ERROR]: Syntax error";
                System.err.println("[ERROR]: Syntax error");
                return;
            }
        }
    }
// feedback will pass to controller and show in the DB server
    public String getFeedback() {
        if (tokenizer.isError == true) {
            feedback = "[ERROR]: Illegal Character or Syntax error";
        }
        return feedback;
    }

    // according first command type pass to different functions
    public void passToCmd(String CT)
    {
        switch(CT) {
            case "USE":
                UseCMD useCmd = new UseCMD();
                use(useCmd);
                break;
            case "CREATE":
                CreateCMD createCmd = new CreateCMD();
                create(createCmd);
                break;
            case "DROP":
                DropCMD dropCMD = new DropCMD();
                drop(dropCMD);
                break;
            case "ALTER":
                AlterCMD alterCMD = new AlterCMD();
                alter(alterCMD);
                break;
            case "INSERT":
                InsertCMD insertCMD = new InsertCMD();
                insert(insertCMD);
                break;
            case "SELECT":
                SelectCMD selectCMD = new SelectCMD();
                select(selectCMD);
                break;
            case "UPDATE":
                UpdateCMD updateCMD = new UpdateCMD();
                update(updateCMD);
                break;
            case "DELETE":
                DeleteCMD deleteCMD = new DeleteCMD();
                delete(deleteCMD);
                break;
            case "JOIN":
                JoinCMD joinCMD = new JoinCMD();
                join(joinCMD);
                break;
            default:
                System.err.println("[ERROR]: Error Command");
        }
    }

    public void use(UseCMD cmd)
    {
        databaseName(cmd);
        if (isEnd()==false) {
            try {
                throw new Exception();
            } catch (Exception e) {
                System.err.println("[ERROR]: Error command ");
            }
        }
        feedback = cmd.query(controller);
    }

    public void create(CreateCMD cmd)
    {
        while(nextToken() != null) {
            switch(tokenType()) {
                case "ST":
                    structure(cmd);
                    break;
                case "BKT":
                    attributeList(cmd);
                    break;
                default:
                    try {
                        throw new Exception();
                    } catch (Exception e) {
                        System.err.println("[ERROR]: Error Command ");
                    }
            }
        }
        feedback = cmd.query(controller);
    }

    public void drop(DropCMD cmd)
    {
        if (nextToken() == null || tokenType() != "ST" ) {
            try {
                throw new Exception();
            } catch (Exception e) {
                System.err.println("[ERROR]: Error Command ");
            }
        }
        structure(cmd);
        feedback = cmd.query(controller);
    }

    public void alter(AlterCMD cmd)
    {
        if (nextToken() != null) {
            if (tokenWord().equals("TABLE")) {
                tableName(cmd);
                if (nextToken() != null) {
                    if (tokenWord().equals("ADD")) {
                        cmd.isAdd = true;
                        attributeList(cmd);
                        feedback = cmd.query(controller);
                        return;
                    }
                    else if (tokenWord().equals("DROP")) {
                        cmd.isAdd = false;
                        attributeList(cmd);
                        feedback = cmd.query(controller);
                        return;
                    }
                }
            }
        }
        try {
             throw new Exception();
        } catch (Exception e) {
             System.err.println("[ERROR]: Error Command ");
        }
    }

    public void insert(InsertCMD cmd) {
        try {
            while (nextToken() != null) {
                switch (tokenType()) {
                    case "KW":
                        if (tokenWord().equals("INTO")) {
                            tableName(cmd);
                            break;
                        }
                        else if (tokenWord().equals("VALUES")) {
                            break;
                        }
                    case "BKT":
                        attributeList(cmd);
                        break;
                    default:
                        throw new Exception();
                }
            }
            feedback = cmd.query(controller);
        } catch (Exception e) {
            e.printStackTrace();
        System.err.println("[ERROR]: Error Command ");
         }
    }

    public void select(SelectCMD cmd)
    {
        try {
                if (nextToken() != null && tokenType().equals("ID")) {
                    if (tokenWord().equals("*")) {
                        cmd.setStar();
                        nextToken();
                    }
                    else {
                        cmd.addColNames(tokenWord());
                        attributeList(cmd);
                    }
                    if (tokenWord().equals("FROM")) {
                        tableName(cmd);
                        if (nextToken() != null) {
                            if (tokenWord().equals("WHERE")) {
                                condition(cmd);
                            }
                            else {
                                feedback = "Syntax error";
                                return;
                            }
                        }
                        feedback = cmd.query(controller);
                        return;
                    }
                }
                throw new Exception();
        } catch (Exception e) {
            System.err.println("[ERROR]: Error Command ");
        }
    }

    public void update(UpdateCMD cmd)
    {
       tableName(cmd);
       if (nextToken() != null && tokenWord().equals("SET")) {
           try {
               nameValuePair(cmd);
               condition(cmd);
               feedback = cmd.query(controller);
           } catch (Exception e) {
               System.err.println("[ERROR]: Error Command ");
               e.printStackTrace();
           }

        }
    }

    public void delete(DeleteCMD cmd)
    {
        if (nextToken() != null && tokenWord().equals("FROM")) {
            try {
                tableName(cmd);
                if (nextToken() != null && tokenWord().equals("WHERE")) {
                    condition(cmd);
                    feedback = cmd.query(controller);
                }
            } catch (Exception e) {
                System.err.println("[ERROR]: Error Command ");
                e.printStackTrace();
            }

        }
    }

    public void join(JoinCMD cmd) {
        try {
            tableName(cmd);
            if (nextToken() != null && tokenWord().equals("AND")) {
                tableName(cmd);
                if (nextToken() != null && tokenWord().equals("ON")) {
                    attributeList(cmd);
                    cmd.setSeparator(cmd.getSizeOfColumn() - 1);
                    if (tokenWord().equals("AND")) {
                        attributeList(cmd);
                        feedback = cmd.query(controller);
                        return;
                    }
                }
            }
            throw new Exception();
        }
        catch (Exception e) {
            System.err.println("Failed to join");
        }
    }

    public  void nameValuePair(UpdateCMD cmd)  throws Exception
    {
        if (nextToken() == null) {
            throw new Exception();
        }
        if (tokenWord().equals("WHERE")) {
            return;
        }
        else if (tokenType().equals("ID")) {
            cmd.addColNames(tokenWord());
            if (nextToken() != null && tokenWord().equals("=")) {
                if (nextToken() != null && (tokenType().equals("INT") || tokenType().equals("DOU") || tokenType().equals("BL") || tokenType().equals("ID"))) {
                    cmd.addValue(tokenWord());
                }
                else {
                    throw new Exception();
                }
            }
            else {
                throw new Exception();
            }
        }
        else {
            throw new Exception();
        }
        nameValuePair(cmd);
    }

    // use reverse polish notation to deal with multiple condition
    public void condition(DBCmd cmd) throws Exception
    {
        ArrayList<String> prefix = toPrefix();
        DBCondition c = null;
        for (int i=0; i<prefix.size(); i++) {
            if (c == null) {
                c = new DBCondition();
            }
            switch (prefix.get(i)) {
                case "col":
                    if (c.getColumn() == null && c.getOp() == null && c.getValue() == null) {
                    c.setColumn(prefix.get(++i));
                    }
                    if (c.getColumn() != null && c.getOp() != null && c.getValue() == null) {
                        c.setValue(prefix.get(++i));
                        cmd.addConditions(c);
                        c = null;
                    }
                    break;
                case "op":
                    c.setOp(prefix.get(++i));
                    break;
                case "value":
                    if (c.getColumn() != null && c.getOp() != null && c.getValue() == null) {
                         c.setValue(prefix.get(++i));
                        cmd.addConditions(c);
                        c = null;
                    }
                    break;
                case "cd":
                    DBCondition c1 = new DBCondition();
                    c1.setType(prefix.get(++i));
                    cmd.addConditions(c1);
                    break;
                default:
                    throw new Exception();
            }
        }

    }

    // turn conditions to prefix notion
    public ArrayList<String> toPrefix() throws Exception
    {
        Stack<String> st = new Stack<>();
        ArrayList<String> a = new ArrayList<>();
        String str = "";
        while (nextToken() != null) {
            switch (tokenType()) {
                case "BKT":
                    if (tokenWord().equals("(")) {
                        st.push(tokenWord());
                    } else {
                        while (!(str = st.pop()).equals("(")) {
                            a.add("cd");
                            a.add(str);
                        }
                    }
                    break;
                case "ID":
                    a.add("col");
                    a.add(tokenWord());
                    break;
                case "OP":
                    a.add("op");
                    a.add(tokenWord());
                    break;
                case "INT":
                case "DOU":
                case "BL":
                    a.add("value");
                    a.add(tokenWord());
                    break;
                case "CD":
                    if (!st.empty()) {
                        while (st.peek().equals("AND") || st.peek().equals("OR")) {
                            a.add("cd");
                            a.add(st.pop());
                        }
                    }
                    st.push(tokenWord());
                    break;
                default:
                    throw new Exception();
            }
        }
        while (!st.empty()) {
            a.add("cd");
            a.add(st.pop());
        }
        return a;
    }

    public void attributeList(DBCmd cmd)
    {
        if (nextToken() == null) {
            return;
        }
        if (tokenType() == "BKT") {
            nextToken();
            return;
        }
        if (!(tokenType() == "ID" || tokenType() == "INT" || tokenType() == "DOU" || tokenType() == "BL" )) {
            return;
        }
        cmd.addColNames(tokenWord());
        attributeList(cmd);
    }

    public void structure(DBCmd cmd)
    {
        if (tokenWord().equals("DATABASE")) {
            databaseName(cmd);
        }
        else if (tokenWord().equals("TABLE")) {
            tableName(cmd);
        }
    }

    public void tableName(DBCmd cmd) {
        try {
            if (nextToken() == null || tokenType() != "ID") {
                throw new Exception();
            }
            cmd.addTableName(tokenWord());
        } catch (Exception e) {
            System.err.println("[ERROR]: Error Command ");
        }
    }

    public void databaseName(DBCmd cmd)
    {
        if (nextToken() == null || tokenType() != "ID") {
            try {
                throw new Exception();
            } catch (Exception e) {
                System.err.println("[ERROR]: Error Command ");
            }
        }
        cmd.setDBName(tokenWord());
    }

    public DBTokenizer.token nextToken()
    {
        return tokenizer.nextToken();
    }

    public boolean isEnd()
    {
        return tokenizer.getIndexOfTokens() >= tokenizer.numOfTokens()-1;
    }

    public DBTokenizer.token getToken()
    {
        return tokenizer.getToken();
    }

    public String tokenType()
    {
        return tokenizer.getToken().type;
    }

    public String tokenWord()
    {
        return tokenizer.getToken().word;
    }

}
