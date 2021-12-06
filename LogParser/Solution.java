package LogParser;

import java.io.IOException;
import java.nio.file.Paths;
import java.text.ParseException;

public class Solution {
    public static void main(String[] args) throws IOException, ParseException {
        LogParser logParser = new LogParser(Paths.get("d:/JAVA/logs/"));
        System.out.println(logParser.execute("get ip for user = \"Eduard Petrovich Morozko\""));
        System.out.println(logParser.execute("get ip"));
        System.out.println(logParser.execute("get user"));
    }
}