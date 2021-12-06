package LogParser;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class LogParser{
    private List<Record> records;
    private SimpleDateFormat formatter = new SimpleDateFormat("dd.MM.yyyy HH:mm:ss");

    public Set<String> getAllUsers() {
        Set<String> set = new HashSet<>();
        for (Record record:records
        ) {
            set.add(record.user);
        }
        return set;
    }

    public int getNumberOfUsers(Date after, Date before) {
        Set<Record> filteredRecords = filteredRecords(after, before);
        Set<String> set = new HashSet<>();
        for (Record record:filteredRecords
        ) {
            set.add(record.user);
        }
        return set.size();
    }

    public int getNumberOfUserEvents(String user, Date after, Date before) {
        Set<Record> filteredRecords = filteredRecords(after, before);
        Set<Event> set = new HashSet<>();
        for (Record record:filteredRecords
        ) {
            if(record.user.equals(user))
            set.add(record.event);
        }
        return set.size();
    }

    public Set<String> getUsersForIP(String ip, Date after, Date before) {
        Set<Record> filteredRecords = filteredRecords(after, before);
        Set<String> set = new HashSet<>();
        for (Record record:filteredRecords
        ) {
            if(record.ip.equals(ip))
                set.add(record.user);
        }
        return set;
    }

    public Set<String> getLoggedUsers(Date after, Date before) {
        Set<Record> filteredRecords = filteredRecords(after, before);
        Set<String> set = new HashSet<>();
        for (Record record:filteredRecords
        ) {
            if(record.event.equals(Event.LOGIN))
                set.add(record.user);
        }
        return set;
    }

    public Set<String> getDownloadedPluginUsers(Date after, Date before) {
        Set<Record> filteredRecords = filteredRecords(after, before);
        Set<String> set = new HashSet<>();
        for (Record record:filteredRecords
        ) {
            if(record.event.equals(Event.DOWNLOAD_PLUGIN))
                set.add(record.user);
        }
        return set;
    }

    public Set<String> getWroteMessageUsers(Date after, Date before) {
        Set<Record> filteredRecords = filteredRecords(after, before);
        Set<String> set = new HashSet<>();
        for (Record record:filteredRecords
        ) {
            if(record.event.equals(Event.WRITE_MESSAGE))
                set.add(record.user);
        }
        return set;
    }

    public Set<String> getSolvedTaskUsers(Date after, Date before) {
        Set<Record> filteredRecords = filteredRecords(after, before);
        Set<String> set = new HashSet<>();
        for (Record record:filteredRecords
        ) {
            if(record.event.equals(Event.SOLVE_TASK))
                set.add(record.user);
        }
        return set;
    }

    public Set<String> getSolvedTaskUsers(Date after, Date before, int task) {
        Set<Record> filteredRecords = filteredRecords(after, before);
        Set<String> set = new HashSet<>();
        for (Record record:filteredRecords
        ) {
            if(record.event.equals(Event.SOLVE_TASK) && record.taskNumber == task)
                set.add(record.user);
        }
        return set;
    }

    public Set<String> getDoneTaskUsers(Date after, Date before) {
        Set<Record> filteredRecords = filteredRecords(after, before);
        Set<String> set = new HashSet<>();
        for (Record record:filteredRecords
        ) {
            if(record.event.equals(Event.DONE_TASK))
                set.add(record.user);
        }
        return set;
    }

    public Set<String> getDoneTaskUsers(Date after, Date before, int task) {
        Set<Record> filteredRecords = filteredRecords(after, before);
        Set<String> set = new HashSet<>();
        for (Record record:filteredRecords
        ) {
            if(record.event.equals(Event.DONE_TASK) && record.taskNumber == task)
                set.add(record.user);
        }
        return set;
    }

    public Set<Date> getDatesForUserAndEvent(String user, Event event, Date after, Date before) {
        Set<Record> filteredRecords = filteredRecords(after, before);
        Set<Date> set = new HashSet<>();
        for (Record record:filteredRecords
        ) {
            if(record.event.equals(event) && record.user.equals(user))
                set.add(record.date);
        }
        return set;
    }

    public Set<Date> getDatesWhenSomethingFailed(Date after, Date before) {
        Set<Record> filteredRecords = filteredRecords(after, before);
        Set<Date> set = new HashSet<>();
        for (Record record:filteredRecords
        ) {
            if(record.status.equals(Status.FAILED))
                set.add(record.date);
        }
        return set;
    }

    public Set<Date> getDatesWhenErrorHappened(Date after, Date before) {
        Set<Record> filteredRecords = filteredRecords(after, before);
        Set<Date> set = new HashSet<>();
        for (Record record:filteredRecords
        ) {
            if(record.status.equals(Status.ERROR))
                set.add(record.date);
        }
        return set;
    }

    public Date getDateWhenUserLoggedFirstTime(String user, Date after, Date before) {
        Set<Record> filteredRecords = filteredRecords(after, before);
        List<Date> list = new ArrayList<>();
        for (Record record:filteredRecords
        ) {
            if(record.event.equals(Event.LOGIN) && record.user.equals(user) && record.status.equals(Status.OK))
                list.add(record.date);
        }
        Collections.sort(list);
        if(list.size() == 0)
            return null;
        else return list.get(0);
    }

    public Date getDateWhenUserSolvedTask(String user, int task, Date after, Date before) {
        Set<Record> filteredRecords = filteredRecords(after, before);
        List<Date> list = new ArrayList<>();
        for (Record record:filteredRecords
        ) {
            if(record.event.equals(Event.SOLVE_TASK) && record.user.equals(user) && record.taskNumber == task)
                list.add(record.date);
        }
        Collections.sort(list);
        if(list.size() == 0)
            return null;
        else return list.get(0);
    }

    public Date getDateWhenUserDoneTask(String user, int task, Date after, Date before) {
        Set<Record> filteredRecords = filteredRecords(after, before);
        List<Date> list = new ArrayList<>();
        for (Record record:filteredRecords
        ) {
            if(record.event.equals(Event.DONE_TASK) && record.user.equals(user) && record.taskNumber == task)
                list.add(record.date);
        }
        Collections.sort(list);
        if(list.size() == 0)
            return null;
        else return list.get(0);
    }

    public Set<Date> getDatesWhenUserWroteMessage(String user, Date after, Date before) {
        Set<Record> filteredRecords = filteredRecords(after, before);
        Set<Date> set = new HashSet<>();
        for (Record record:filteredRecords
        ) {
            if(record.event.equals(Event.WRITE_MESSAGE) && record.user.equals(user))
                set.add(record.date);
        }
        return set;
    }

    public Set<Date> getDatesWhenUserDownloadedPlugin(String user, Date after, Date before) {
        Set<Record> filteredRecords = filteredRecords(after, before);
        Set<Date> set = new HashSet<>();
        for (Record record:filteredRecords
        ) {
            if(record.event.equals(Event.DOWNLOAD_PLUGIN) && record.user.equals(user))
                set.add(record.date);
        }
        return set;
    }

    public int getNumberOfAllEvents(Date after, Date before) {
        Set<Record> filteredRecords = filteredRecords(after, before);
        Set<Event> set = new HashSet<>();
        for (Record record:filteredRecords
        ) {
                set.add(record.event);
        }
        return set.size();
    }

    public Set<Event> getAllEvents(Date after, Date before) {
        Set<Record> filteredRecords = filteredRecords(after, before);
        Set<Event> set = new HashSet<>();
        for (Record record:filteredRecords
        ) {
            set.add(record.event);
        }
        return set;
    }

    public Set<Event> getEventsForIP(String ip, Date after, Date before) {
        Set<Record> filteredRecords = filteredRecords(after, before);
        Set<Event> set = new HashSet<>();
        for (Record record:filteredRecords
        ) {
            if(record.ip.equals(ip))
                set.add(record.event);
        }
        return set;
    }

    public Set<Event> getEventsForUser(String user, Date after, Date before) {
        Set<Record> filteredRecords = filteredRecords(after, before);
        Set<Event> set = new HashSet<>();
        for (Record record:filteredRecords
        ) {
            if(record.user.equals(user))
                set.add(record.event);
        }
        return set;
    }

    public Set<Event> getFailedEvents(Date after, Date before) {
        Set<Record> filteredRecords = filteredRecords(after, before);
        Set<Event> set = new HashSet<>();
        for (Record record:filteredRecords
        ) {
            if(record.status.equals(Status.FAILED))
                set.add(record.event);
        }
        return set;
    }

    public Set<Event> getErrorEvents(Date after, Date before) {
        Set<Record> filteredRecords = filteredRecords(after, before);
        Set<Event> set = new HashSet<>();
        for (Record record:filteredRecords
        ) {
            if(record.status.equals(Status.ERROR))
                set.add(record.event);
        }
        return set;
    }

    public int getNumberOfAttemptToSolveTask(int task, Date after, Date before) {
        Set<Record> filteredRecords = filteredRecords(after, before);
        List<Event> list = new ArrayList<>();
        for (Record record:filteredRecords
        ) {
            if(record.event.equals(Event.SOLVE_TASK) && record.taskNumber == task)
                list.add(record.event);
        }
        return list.size();
    }

    public int getNumberOfSuccessfulAttemptToSolveTask(int task, Date after, Date before) {
        Set<Record> filteredRecords = filteredRecords(after, before);
        List<Event> list = new ArrayList<>();
        for (Record record:filteredRecords
        ) {
            if(record.event.equals(Event.DONE_TASK) && record.taskNumber == task)
                list.add(record.event);
        }
        return list.size();
    }

    public Map<Integer, Integer> getAllSolvedTasksAndTheirNumber(Date after, Date before) {
        Set<Record> filteredRecords = filteredRecords(after, before);
        Map<Integer, Integer> mapa = new HashMap<>();
        int att = 0;
        for (Record record:filteredRecords
        ) {
            if(record.taskNumber != null && record.event.equals(Event.SOLVE_TASK)){
                if(!mapa.containsKey(record.taskNumber)) {
                    mapa.put(record.taskNumber, 1);
                }else {
                    mapa.put(record.taskNumber, mapa.get(record.taskNumber) + 1);
                }
            }
        }
        return mapa;
    }

    public Map<Integer, Integer> getAllDoneTasksAndTheirNumber(Date after, Date before) {
        Set<Record> filteredRecords = filteredRecords(after, before);
        Map<Integer, Integer> mapa = new HashMap<>();
        int att = 0;
        for (Record record:filteredRecords
        ) {
            if(record.taskNumber != null && record.event.equals(Event.DONE_TASK)){
                if(!mapa.containsKey(record.taskNumber)) {
                    mapa.put(record.taskNumber, 1);
                }else {
                    mapa.put(record.taskNumber, mapa.get(record.taskNumber) + 1);
                }
            }
        }
        return mapa;
    }

    public Set<Object> execute(String query){
        Set<Object> set = new HashSet<>();
        Set<String> setString = new HashSet<>();
        Set<Date> setDate = new HashSet<>();
        Set<Event> setEvent = new HashSet<>();
        Set<Status> setStatus = new HashSet<>();
        Set<Integer> setTasks = new HashSet<>();
        String[] properties = query.split(" ");
        String value = null;
        String str = null;
        try {
            Pattern pattern = Pattern.compile("\"[\\d\\w][^\"]*[\\d\\w]\"");
            Matcher matcher = pattern.matcher(query);
            List<String> listOfProperties = new ArrayList<>();
            while (matcher.find()) {
                listOfProperties.add(matcher.group().replaceAll("\"", ""));
            }
            try {
                str = properties[1] + " " + properties[3];
            } catch (ArrayIndexOutOfBoundsException e) {
                switch (query) {
                    case "get ip":
                        setString = getUniqueIPs(null, null);
                        return new HashSet<>(setString);
                    case "get user":
                        setString = getAllUsers();
                        return new HashSet<>(setString);
                    case "get date":
                        for (Record record : records
                        ) {
                            setDate.add(record.date);
                        }
                        return new HashSet<>(setDate);
                    case "get event":
                        setEvent = getAllEvents(null, null);
                        return new HashSet<>(setEvent);
                    case "get status":
                        for (Record record : records
                        ) {
                            setStatus.add(record.status);
                        }
                        return new HashSet<>(setStatus);
                }
            }
            if (listOfProperties.size() == 1) {
                value = listOfProperties.get(0);
                switch (str) {
                    case "ip user":
                        for (Record record : records
                        ) {
                            if (record.user.equals(value))
                                setString.add(record.ip);
                        }
                        return new HashSet<>(setString);
                    case "ip date":
                        for (Record record : records
                        ) {
                            if (record.date.equals(formatter.parse(value)))
                                setString.add(record.ip);
                        }
                        return new HashSet<>(setString);
                    case "ip event":
                        for (Record record : records
                        ) {
                            if (record.event.equals(Event.valueOf(value)))
                                setString.add(record.ip);
                        }
                        return new HashSet<>(setString);
                    case "ip status":
                        for (Record record : records
                        ) {
                            if (record.status.equals(Status.valueOf(value)))
                                setString.add(record.ip);
                        }
                        return new HashSet<>(setString);
                    case "user ip":
                        for (Record record : records
                        ) {
                            if (record.ip.equals(value))
                                setString.add(record.user);
                        }
                        return new HashSet<>(setString);
                    case "user date":
                        for (Record record : records
                        ) {
                            if (record.date.equals(formatter.parse(value)))
                                setString.add(record.user);
                        }
                        return new HashSet<>(setString);
                    case "user event":
                        for (Record record : records
                        ) {
                            if (record.event.equals(Event.valueOf(value)))
                                setString.add(record.user);
                        }
                        return new HashSet<>(setString);
                    case "user status":
                        for (Record record : records
                        ) {
                            if (record.status.equals(Status.valueOf(value)))
                                setString.add(record.user);
                        }
                        return new HashSet<>(setString);
                    case "date ip":
                        for (Record record : records
                        ) {
                            if (record.ip.equals(value))
                                setDate.add(record.date);
                        }
                        return new HashSet<>(setDate);
                    case "date user":
                        for (Record record : records
                        ) {
                            if (record.user.equals(value))
                                setDate.add(record.date);
                        }
                        return new HashSet<>(setDate);
                    case "date event":
                        for (Record record : records
                        ) {
                            if (record.event.equals(Event.valueOf(value)))
                                setDate.add(record.date);
                        }
                        return new HashSet<>(setDate);
                    case "date status":
                        for (Record record : records
                        ) {
                            if (record.status.equals(Status.valueOf(value)))
                                setDate.add(record.date);
                        }
                        return new HashSet<>(setDate);
                    case "event ip":
                        for (Record record : records
                        ) {
                            if (record.ip.equals(value))
                                setEvent.add(record.event);
                        }
                        return new HashSet<>(setEvent);
                    case "event user":
                        for (Record record : records
                        ) {
                            if (record.user.equals(value))
                                setEvent.add(record.event);
                        }
                        return new HashSet<>(setEvent);
                    case "event date":
                        for (Record record : records
                        ) {
                            if (record.date.equals(formatter.parse(value)))
                                setEvent.add(record.event);
                        }
                        return new HashSet<>(setEvent);
                    case "event status":
                        for (Record record : records
                        ) {
                            if (record.status.equals(Status.valueOf(value)))
                                setEvent.add(record.event);
                        }
                        return new HashSet<>(setEvent);
                    case "status ip":
                        for (Record record : records
                        ) {
                            if (record.ip.equals(value))
                                setStatus.add(record.status);
                        }
                        return new HashSet<>(setStatus);
                    case "status user":
                        for (Record record : records
                        ) {
                            if (record.user.equals(value))
                                setStatus.add(record.status);
                        }
                        return new HashSet<>(setStatus);
                    case "status date":
                        for (Record record : records
                        ) {
                            if (record.date.equals(formatter.parse(value)))
                                setStatus.add(record.status);
                        }
                        return new HashSet<>(setStatus);
                    case "status event":
                        for (Record record : records
                        ) {
                            if (record.event.equals(Event.valueOf(value)))
                                setStatus.add(record.status);
                        }
                        return new HashSet<>(setStatus);
                }
            } else {
                Set<Record> filteredRecords = filteredRecordsExcept(formatter.parse(listOfProperties.get(1)), formatter.parse(listOfProperties.get(2)));
                value = listOfProperties.get(0);
                switch (str) {
                            case "ip user":
                                for (Record record : filteredRecords
                                ) {
                                    if (record.user.equals(value))
                                        setString.add(record.ip);
                                }
                                return new HashSet<>(setString);
                            case "ip date":
                                for (Record record : filteredRecords
                                ) {
                                    if (record.date.equals(formatter.parse(value)))
                                        setString.add(record.ip);
                                }
                                return new HashSet<>(setString);
                            case "ip event":
                                for (Record record : filteredRecords
                                ) {
                                    if (record.event.equals(Event.valueOf(value)))
                                        setString.add(record.ip);
                                }
                                return new HashSet<>(setString);
                            case "ip status":
                                for (Record record : filteredRecords
                                ) {
                                    if (record.status.equals(Status.valueOf(value)))
                                        setString.add(record.ip);
                                }
                                return new HashSet<>(setString);
                            case "user ip":
                                for (Record record : filteredRecords
                                ) {
                                    if (record.ip.equals(value))
                                        setString.add(record.user);
                                }
                                return new HashSet<>(setString);
                            case "user date":
                                for (Record record : filteredRecords
                                ) {
                                    if (record.date.equals(formatter.parse(value)))
                                        setString.add(record.user);
                                }
                                return new HashSet<>(setString);
                            case "user event":
                                for (Record record : filteredRecords
                                ) {
                                    if (record.event.equals(Event.valueOf(value)))
                                        setString.add(record.user);
                                }
                                return new HashSet<>(setString);
                            case "user status":
                                for (Record record : filteredRecords
                                ) {
                                    if (record.status.equals(Status.valueOf(value)))
                                        setString.add(record.user);
                                }
                                return new HashSet<>(setString);
                            case "date ip":
                                for (Record record : filteredRecords
                                ) {
                                    if (record.ip.equals(value))
                                        setDate.add(record.date);
                                }
                                return new HashSet<>(setDate);
                            case "date user":
                                for (Record record : filteredRecords
                                ) {
                                    if (record.user.equals(value))
                                        setDate.add(record.date);
                                }
                                return new HashSet<>(setDate);
                            case "date event":
                                for (Record record : filteredRecords
                                ) {
                                    if (record.event.equals(Event.valueOf(value)))
                                        setDate.add(record.date);
                                }
                                return new HashSet<>(setDate);
                            case "date status":
                                for (Record record : filteredRecords
                                ) {
                                    if (record.status.equals(Status.valueOf(value)))
                                        setDate.add(record.date);
                                }
                                return new HashSet<>(setDate);
                            case "event ip":
                                for (Record record : filteredRecords
                                ) {
                                    if (record.ip.equals(value))
                                        setEvent.add(record.event);
                                }
                                return new HashSet<>(setEvent);
                            case "event user":
                                for (Record record : filteredRecords
                                ) {
                                    if (record.user.equals(value))
                                        setEvent.add(record.event);
                                }
                                return new HashSet<>(setEvent);
                            case "event date":
                                for (Record record : filteredRecords
                                ) {
                                    if (record.date.equals(formatter.parse(value)))
                                        setEvent.add(record.event);
                                }
                                return new HashSet<>(setEvent);
                            case "event status":
                                for (Record record : filteredRecords
                                ) {
                                    if (record.status.equals(Status.valueOf(value)))
                                        setEvent.add(record.event);
                                }
                                return new HashSet<>(setEvent);
                            case "status ip":
                                for (Record record : filteredRecords
                                ) {
                                    if (record.ip.equals(value))
                                        setStatus.add(record.status);
                                }
                                return new HashSet<>(setStatus);
                            case "status user":
                                for (Record record : filteredRecords
                                ) {
                                    if (record.user.equals(value))
                                        setStatus.add(record.status);
                                }
                                return new HashSet<>(setStatus);
                            case "status date":
                                for (Record record : filteredRecords
                                ) {
                                    if (record.date.equals(formatter.parse(value)))
                                        setStatus.add(record.status);
                                }
                                return new HashSet<>(setStatus);
                            case "status event":
                                for (Record record : filteredRecords
                                ) {
                                    if (record.event.equals(Event.valueOf(value)))
                                        setStatus.add(record.status);
                                }
                                return new HashSet<>(setStatus);
                        }
            }
        }catch (ParseException e){
            e.printStackTrace();
        }
        return set;
    }

    private class Record{
        String ip;
        String user;
        Date date;
        Event event;
        Integer taskNumber;
        Status status;

        @Override
        public String toString() {
            return "Record{" +
                    "ip='" + ip + '\'' +
                    ", user='" + user + '\'' +
                    ", date=" + date +
                    ", event=" + event +
                    ", taskNumber=" + taskNumber +
                    ", status=" + status +
                    '}';
        }
    }

    public LogParser(Path logDir) {
        records = new ArrayList<>();
        readRecords(logDir);
    }

    private void readRecords(Path logDir){
        try{
            DirectoryStream<Path> directoryStream = Files.newDirectoryStream(logDir);
            for (Path log: directoryStream
                 ) {
                if(Files.isRegularFile(log) && log.toString().endsWith(".log")){
                    BufferedReader bufferedReader = Files.newBufferedReader(log, StandardCharsets.UTF_8);
                    while (bufferedReader.ready()){
                        Record record = new Record();
                        String[] entry = bufferedReader.readLine().split("\t");
                        record.ip = entry[0];
                        record.user = entry[1];
                        record.date = formatter.parse(entry[2]);
                        if (entry[3].indexOf(' ') == -1) {
                            record.event = Event.valueOf(entry[3]);
                            record.taskNumber = null;
                        } else {
                            String[] event = entry[3].split(" ");
                            record.event = Event.valueOf(event[0]);
                            record.taskNumber = Integer.parseInt(event[1]);
                        }
                        record.status = Status.valueOf(entry[4]);
                        records.add(record);
                    }
                    bufferedReader.close();
                }else{
                    if(Files.isDirectory(log))
                        readRecords(log);
                }
            }
        }catch (IOException | ParseException e){
            e.printStackTrace();
        }
    }

    private Set<Record> filteredRecords(Date after, Date before){
        Set<Record> set = new HashSet<>();
        for (Record record:records
             ) {
            if(isBetween(record.date, after, before))
                set.add(record);
        }
        return set;
    }

    private Set<Record> filteredRecordsExcept(Date after, Date before){
        Set<Record> set = new HashSet<>();
        for (Record record:records
        ) {
            if(isBetweenExcept(record.date, after, before))
                set.add(record);
        }
        return set;
    }

    private boolean isBetween(Date date, Date after, Date before) {
        return (after == null || date.after(after) || date.equals(after)) &&
                (before == null || date.before(before) || date.equals(before));
    }

    private boolean isBetweenExcept(Date date, Date after, Date before) {
        return (after == null || date.after(after)) &&
                (before == null || date.before(before));
    }

    public int getNumberOfUniqueIPs(Date after, Date before) {
        Set<String> set = getUniqueIPs(after, before);
        return set.size();
    }

    public Set<String> getUniqueIPs(Date after, Date before) {
        Set<Record> filteredRecords = filteredRecords(after, before);
        Set<String> set = new HashSet<>();
        for (Record record:filteredRecords
            ) {
               set.add(record.ip);
            }
        return set;
    }

    public Set<String> getIPsForUser(String user, Date after, Date before) {
        Set<Record> filteredRecords = filteredRecords(after, before);
        Set<String> set = new HashSet<>();
        for (Record record:filteredRecords
             ) {
            if(record.user.equals(user))
                set.add(record.ip);
        }
        return set;
    }

    public Set<String> getIPsForEvent(Event event, Date after, Date before) {
        Set<Record> filteredRecords = filteredRecords(after, before);
        Set<String> set = new HashSet<>();
        for (Record record:filteredRecords
        ) {
            if(record.event.equals(event))
                set.add(record.ip);
        }
        return set;
    }

    public Set<String> getIPsForStatus(Status status, Date after, Date before) {
        Set<Record> filteredRecords = filteredRecords(after, before);
        Set<String> set = new HashSet<>();
        for (Record record:filteredRecords
        ) {
            if(record.status.equals(status))
                set.add(record.ip);
        }
        return set;
    }
}