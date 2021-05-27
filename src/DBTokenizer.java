import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Arrays;

/**
 * @author lee
 * @create 2021-03-17 19:09
 */
public class DBTokenizer {
        private String statement;
        private token[] tokens;
        private int indexOfTokens;
        public boolean isError = false;

        public void tokenizing(String statement)
        {
            isError = false;
            try {
                split(statement);
                passToTokens();
            } catch (Exception e) {
                isError = true;
                System.err.println("[ERROR]: Syntax error");
            }
        }

        public void split(String statement) throws Exception
        {
            int index;
            if ((index = statement.indexOf(';')) == -1) {
                throw new Exception();
            }
            this.statement = statement.substring(0,index);
            this.indexOfTokens = 0;
        }

        public ArrayList<String> toWords() throws Exception
        {

            String[] statementSplit = this.statement.replaceAll("[(]", " ( ").replaceAll("[)]", " ) ").split("[,\\s]+");
            ArrayList<String> wordSplits = new ArrayList<>();
            if(statementSplit.length == 0) {
                isError = true;
                System.err.println("[ERROR]: Invalid input ");
            }
//            System.out.println(Arrays.toString(statementSplit));
            for (int i=0; i<statementSplit.length; i++) {
                if (statementSplit[i].charAt(0) != '\'') {
                    wordSplits.add(statementSplit[i]);
                }
                else if (statementSplit[i].charAt(0) == '\''  && statementSplit[i].charAt(statementSplit[i].length()-1) == '\'' ) {
                    wordSplits.add(statementSplit[i].replaceAll("'",""));
                }
                else {
                    String s = statementSplit[i].replaceAll("'","");
                    while(statementSplit[++i].charAt(statementSplit[i].length()-1) != '\'') {
                        s += " " + statementSplit[i];
                        if (i == statementSplit.length-1) {
                            throw new Exception();
                        }
                    }
                    s += " " + statementSplit[i].replaceAll("'","");
                    wordSplits.add(s);
                }
            }
            System.out.println(wordSplits);
            return wordSplits;
        }

        public void passToTokens()
        {
//            String[] statementSplit = this.statement.replaceAll("[(]", " (  ").replaceAll("[)]", " ) ").replaceAll(",", " ").split("(?<!'[^\\s]{1,5})[\\s]+(?![^\\s]+')");
//        String[] statementSplit = this.statement.replaceAll("[(]", " ( ").replaceAll("[)]", " ) ").split("[,\\s]+");
//           System.out.println(Arrays.toString(statementSplit));
//            for (int i=0; i<statementSplit.length; i++) {
//                statementSplit[i] = statementSplit[i].replaceAll("'|^\\s|\\s$","");
//            }
            ArrayList<String> wordSplits = null;
            try {
                wordSplits = toWords();
            } catch (Exception e) {
                isError = true;
            }
            this.tokens = new token[wordSplits.size()];
            try{
                for (int i=0; i<wordSplits.size(); i++) {
                    tokens[i] = new token(wordSplits.get(i));
                }
            }
            catch (Exception e) {
                isError = true;
                System.err.println("[ERROR]: Illegal Character ");
            }
        }

        public token nextToken()
        {
            if (this.indexOfTokens >= (tokens.length -1) ) {
                return null;
            }
            else {
                return tokens[++indexOfTokens];
            }
        }

        public int getIndexOfTokens()
        {
            return this.indexOfTokens;
        }

        public int numOfTokens()
        {
            return tokens.length;
        }

        public token getToken()
        {
            return tokens[indexOfTokens];
        }

        public String toString()
        {
            if (tokens == null || tokens.length == 0) {
                return "";
            }
            String line = this.statement + '\n';
            for (int i=0; i< tokens.length; i++) {

                line += "[ " + tokens[i].type + " : " + tokens[i].word + " ]";
            }
            return line;
        }

         class token  {
            String word;
            String type;

            String CT = "^USE$|^CREATE$|^DROP$|^ALTER$|^INSERT$|^SELECT$|^UPDATE$|^DELETE$|^JOIN$";
            String ST = "^TABLE$|^DATABASE$";
            String KW = "^FROM$|^INTO$|^SET$|^WHERE$|^ON$|^VALUES$|^ADD$";
            String OP = "^=$|^==$|^>$|^<$|^>=$|^<=$|^!=$|^LIKE$";
            String CD = "^AND$|^OR$";
            String BL = "^TRUE$|^FALSE$";
//            String BKT = "^\\([\\s\\S]*\\)$";
            String BKT = "^\\($|^\\)$";
            String INT = "^\\d+$";
            String DOU = "^(-?\\d+)(.\\d+)?$";
            String ID = "^[A-Za-z0-9-*'_\\s]+$";


              token (String word) throws Exception
              {
                    this.word = word.toUpperCase();
//                    System.out.println(word);
                    if (this.word.matches(CT)) {
                        this.type = "CT";
                    }
                    else if (this.word.matches(ST)) {
                        this.type = "ST";
                    }
                    else if (this.word.matches(KW)) {
                        this.type = "KW";
                    }
                    else if (this.word.matches(CD)) {
                        this.type = "CD";
                    }
                    else if (this.word.matches(INT)) {
                        this.type = "INT";
                    }
                    else if (this.word.matches(BKT)) {
                        this.type = "BKT";
                    }
                    else if (this.word.matches(DOU)) {
                        this.type = "DOU";
                    }
                    else if (this.word.matches(BL)) {
                        this.word = word;
                        this.type = "BL";
                    }
                    else if (this.word.matches(OP)) {
                        this.type = "OP";
                    }
                    else if (this.word.matches(ID)) {
                        this.word = word;
                        this.type = "ID";
                    }
                    else {
                        throw new Exception();
                    }
              }
        }

    public static void main(String[] args)
    {
        DBTokenizer tk = null;
        tk = new DBTokenizer();
             tk.tokenizing("INSERT INTO actors VALUES ('Hugh Grant', 'British', 3);");
        System.out.println(tk);
    }
}
