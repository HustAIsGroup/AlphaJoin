// Used to collect training data of the value network
import java.io.*;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.*;

import static java.lang.Thread.sleep;

public class getruntimerandom {

    static final String tablenameDir = "../resource/jobtablename";
    static final String queryDir = "../resource/jobquery";
    static final String shortDir = "../resource/shorttolong";
    static final String outputDir = "./t6.sql";
    static final String queryplanDir = "../data";
    static final String queryEncodedDictDir = "./queryEncodedDict";
    static final int tableNumber = 28;
    
     public static void main(String[] args) throws IOException, SQLException, InterruptedException {
        String[] fileList = new File(tablenameDir).list();
        System.out.println(fileList.length);
        Arrays.sort(fileList);

        HashSet<String> generateSet = new HashSet<>();
        HashMap<String, int[]> joinMatrixDict = new HashMap<>();
        ArrayList<String> table2int = new ArrayList<>();
        HashSet<String> historySet = new HashSet<>();
        prepareData(joinMatrixDict, table2int, historySet);

        BufferedWriter bw = path2bwa(outputDir);
        while (true) {
            for (int i = 0; i < fileList.length; i++) {
                //1.get number of join, query contact
                String queryName = fileList[i];
                String[] tables = getQueryTables(queryName);
                int joinNumber = tables.length - 1;

                //2.make hint
                generateSet.clear();
                int[] state = joinMatrixDict.get(queryName).clone();
                int[] matrix = new int[tableNumber * tableNumber];
                generatematrix(state, matrix, joinNumber,
                        generateSet, tables, table2int);

                //3.run hint query
                //System.out.println(queryName +","+generateSet.size());
                if (joinNumber < 5) {
                    for (String line : generateSet) {
                        if (historySet.contains(queryName + line))
                            continue;
                        historySet.add(queryName + line);
                        //System.out.println(queryName + line);
                        runQuery(queryName, line, bw);
                    }
                } else {
                    int count = 0;
                    for (String line : generateSet) {
                        if (historySet.contains(queryName + line))
                            continue;
                        historySet.add(queryName + line);
                        runQuery(queryName, line, bw);
                        //System.out.println(queryName + line);
                        count += 1;
                        if (count > 25)
                            break;
                    }


                }
            }
        }
    }

    static class Action {
        int x;
        int y;

        Action(int x, int y) {
            this.x = x;
            this.y = y;
        }
    }

    public static void runQuery(String queryName, String hintCore, BufferedWriter bw)
            throws InterruptedException, IOException, SQLException {
        String queryContact = getQueryContent(queryName);
        sleep(1000);
        Process process = Runtime.getRuntime().exec("./dropCache.sh");
        process.waitFor();
        SimpleDateFormat df = new SimpleDateFormat("MM-dd HH:mm:ss");
        Connection connection = JDBCUtil.getConnection();
        Statement statement = connection.createStatement();
        String hint = "/*+ Leading(" + hintCore + ") */ \n explain analyze \n ";
        String sql = hint + queryContact;
        statement.setQueryTimeout(600);
        ResultSet rs = null;
        try {
            rs = statement.executeQuery(sql);
            rs.next();
            //System.out.println(rs.getString(1));
            double cost = Double.valueOf(rs.getString(1).split("=")[4].split("[..]")[0]);
            ArrayList<String> queryplan = new ArrayList<>();
            while (rs.next()) {
                queryplan.add(rs.getString(1));
            }
            String realOrder = plan2hint(queryplan, 0, queryplan.size());
            String output = queryName + "," + hintCore + "," + cost + "," + realOrder +
                    "," + (realOrder.equals(hintCore)) + "," + df.format(new Date());
            System.out.println(output);
            bw.write(output + "\n");
            rs.close();
            BufferedWriter ba = path2bw(queryplanDir + "/" + queryName + " " + hintCore + ".txt");
            for (String lineOfPlan : queryplan) {
                ba.write(lineOfPlan + "\n");
            }
            ba.flush();
            ba.close();
        } catch (SQLException e) {
            String output = queryName + "," + hintCore + "," + "timeout,-," + df.format(new Date());
            System.out.println(output);
            bw.write(output + "\n");
        } finally {
            bw.flush();
            statement.close();
            connection.close();
        }
    }

    public static String[] getQueryTables(String queryName) throws IOException {
        FileReader frtn = new FileReader(tablenameDir + "/" + queryName);
        BufferedReader brtn = new BufferedReader(frtn);
        String tablestemp = brtn.readLine();
        String[] tables = tablestemp.substring(2, tablestemp.length() - 2).split("', '");
        brtn.close();
        frtn.close();
        return tables;
    }

    public static String getQueryContent(String queryName) throws IOException {
        StringBuilder sb = new StringBuilder();
        FileReader fr = new FileReader(queryDir + "/" + queryName + ".sql");
        BufferedReader br = new BufferedReader(fr);
        String qline;
        while ((qline = br.readLine()) != null) {
            sb.append(qline + "\n");
        }
        br.close();
        fr.close();
        return sb.toString();
    }

    public static void matrixToHint(int[] matrix, HashSet<String> hashSet, String[] tables,
                                   ArrayList<String> table2int) {
        //decoed matrix to hint
        HashMap<String, String> tempdect = new HashMap<>();
        for (String tab : tables) {
            tempdect.put(tab, tab);
        }

        int correctcount = 0;
        int index = 0;
        while (correctcount != tables.length - 1) {
            int max = 0;
            for (int i1 = 0; i1 < matrix.length; i1++) {
                if (matrix[i1] > max) {
                    max = matrix[i1];
                    index = i1;
                }
            }
            matrix[index] = 0;
            int indexa = index / tableNumber;
            int indexb = index % tableNumber;
            if (tempdect.get(table2int.get(indexa)).equals(tempdect.get(table2int.get(indexb)))) {
                return;
            }
            String string = "( " + tempdect.get(table2int.get(indexa)) + " " + tempdect.get(table2int.get(indexb)) + " )";
            correctcount += 1;
            for (String j : string.split(" "))
                if (tempdect.containsKey(j))
                    tempdect.put(j, string);
        }
        //System.out.println(tempdect.get(tables[0]));
        hashSet.add(tempdect.get(tables[0]));

    }

    public static void generatematrix(int[] state, int[] matrix, int joinNumber,
                                     HashSet<String> hashSet, String[] tables, ArrayList<String> table2int) {
        if (joinNumber == 0) {
            int[] temp = matrix.clone();
            matrixToHint(temp, hashSet, tables, table2int);
        }
        ArrayList<Action> possibleActions = new ArrayList<>();
        for (int x = 0; x < tableNumber; x++) {
            for (int y = 0; y < tableNumber; y++) {
                if (state[x * tableNumber + y] == 1)
                    possibleActions.add(new Action(x, y));
            }
        }
        for (int i = 0; i < possibleActions.size(); i++) {
            int x = possibleActions.get(i).x;
            int y = possibleActions.get(i).y;
            matrix[x * tableNumber + y] = joinNumber;
            state[x * tableNumber + y] = 0;
            state[y * tableNumber + x] = 0;
            generatematrix(state, matrix, joinNumber - 1, hashSet, tables, table2int);
            matrix[x * tableNumber + y] = 0;
            state[x * tableNumber + y] = 1;
            state[y * tableNumber + x] = 1;
        }
    }

    public static void prepareData(HashMap<String, int[]> joinMatrixDict,
                                   ArrayList<String> table2int,
                                   HashSet<String> historySet) throws IOException {
        String dict = null;
        String short2long = null;
        dict = new BufferedReader(new FileReader(queryEncodedDictDir)).readLine();
        dict = dict.substring(2, dict.length() - 2);
        short2long = new BufferedReader(new FileReader(shortDir)).readLine();
        short2long = short2long.substring(2, short2long.length() - 2);

        String[] encodes = dict.split("], '");
        for (String aaa : encodes) {
            String key = aaa.split("': \\[")[0];
            String value = aaa.split("': \\[")[1];
            int[] val = new int[tableNumber * tableNumber];
            for (int i = 0; i < val.length; i++) {
                if (value.charAt(i * 3) == '0') {
                    val[i] = 0;
                } else {
                    val[i] = 1;
                }
            }
            joinMatrixDict.put(key, val);
        }

        String[] shortnames = short2long.split("', '");
        for (String bbb : shortnames) {
            table2int.add(bbb.split("': '")[0]);
        }
        table2int.sort(Comparator.naturalOrder());

        BufferedReader brt = new BufferedReader(new FileReader(outputDir));
        String oneline;
        while ((oneline = brt.readLine()) != null) {
            String queryHint = oneline.split(",")[0] + oneline.split(",")[1];
            if (!historySet.contains(queryHint)) {
                historySet.add(queryHint);
            }
        }
        brt.close();
    }

    public static void swap(String[] tablenames, int i, int j) {
        String temp = tablenames[i];
        tablenames[i] = tablenames[j];
        tablenames[j] = temp;
    }

    public static BufferedWriter path2bw(String path) throws IOException {
        File f = new File(path);
        if (!f.exists()) {
            File dir = new File(f.getParent());
            dir.mkdir();
            f.createNewFile();
        }
        FileWriter fw = new FileWriter(f);
        BufferedWriter bw = new BufferedWriter(fw);
        return bw;
    }

    public static BufferedWriter path2bwa(String path) throws IOException {
        File f = new File(path);
        if (!f.exists()) {
            File dir = new File(f.getParent());
            dir.mkdir();
            f.createNewFile();
        }
        FileWriter fw = new FileWriter(f, true);
        BufferedWriter bw = new BufferedWriter(fw);
        return bw;
    }

    public static String plan2hint(ArrayList<String> queryplan, int begin, int end) {
        if (begin == end) {
            return "";
        }
        int blk = blank(queryplan.get(begin));
        for (int i = begin + 1; i < end; i++) {
            if (blank(queryplan.get(i)) == blk && blank(queryplan.get(i)) != -1) {
                String b = plan2hint(queryplan, i, end);
                String a = plan2hint(queryplan, begin + 1, i);
                if (queryplan.get(begin).contains("Scan") && !queryplan.get(begin).contains("Bitmap Index")) {
                    String language = queryplan.get(begin);
                    String[] word = language.split(" ");
                    for (int j = 0; j < word.length; j++) {
                        if (word[j].equals("on"))
                            a = word[j + 2];
                    }
                }
                return "( " + a + " " + b + " )";
            }
        }
        if (queryplan.get(begin).contains("Scan") && !queryplan.get(begin).contains("Bitmap Index")) {
            String language = queryplan.get(begin);
            String[] word = language.split(" ");
            for (int j = 0; j < word.length; j++) {
                if (word[j].equals("on"))
                    return word[j + 2];
            }
        }
        return plan2hint(queryplan, begin + 1, end);
    }

    public static int blank(String line) {
        for (int i = 0; i < line.length() - 1; i++)
            if (line.charAt(i) == '-' && line.charAt(i + 1) == '>')
                return i;
        return -1;
    }

}
