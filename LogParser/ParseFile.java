package LogParser;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.stream.Collectors;

public class ParseFile {
    public static List<String> listOfStringsFromFile(String fileName) throws IOException {
        BufferedReader br = new BufferedReader(new FileReader(fileName));
        List<String> list = new ArrayList<>();
        String str = null;
        while ((str = br.readLine()) != null)
            list.add(str);
        return list;
    }

    public static List<String> listOfPropertiesFromString(String str){
        List<String> list = new ArrayList<>();
        list = Arrays.stream(str.split("\t")).collect(Collectors.toList());
        return list;
    }

    public static Date dateFromString(String str){
        Date date = new Date();
        try{
            SimpleDateFormat simpleDateFormat = new SimpleDateFormat("dd.MM.yyyy HH:mm:ss");
            date = simpleDateFormat.parse(str);
        }catch (ParseException e){
            System.out.println("Неправильный формат даты");
        }
        return date;
    }
}
