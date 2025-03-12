import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.io.IOException;
import java.io.File;
import java.io.FileReader;
import java.io.BufferedReader;
import java.util.Scanner;

public class MapReduceFiles {

    //        if (args.length < 3) {
    //            System.err.println("usage: java MapReduceFiles file1.txt file2.txt file3.txt");
    //        }

    //        Map<String, String> input = new HashMap<String, String>();
    //        for (int i = 0; i < numToProcess; i++) {
    //
    //        }
    //        try {
    //            input.put(args[0], readFile(args[0]));
    //            input.put(args[1], readFile(args[1]));
    //            input.put(args[2], readFile(args[2]));
    //        }
    //        catch (IOException ex)
    //        {
    //            System.err.println("Error reading files...\n" + ex.getMessage());
    //            ex.printStackTrace();
    //            System.exit(0);
    //        }

    public static void main(String[] args) {
        Scanner scanner = new Scanner(System.in);
        Map<String, String> input = new HashMap<String, String>();

        if (args.length < 1) {
            System.err.println("Invalid number of arguments");
            System.exit(1);
        }

        String[] filePaths = args;  // Get file paths from command-line arguments
        int numToProcess = filePaths.length;

        System.out.println("Processing " + numToProcess + " files");

        // Read the provided files
        for (String filePath : filePaths) {
            try {
                input.put(filePath, readFile(filePath));
            } catch (IOException ex) {
                System.err.println("Error reading file: " + filePath);
                ex.printStackTrace();
                System.exit(1);
            }
        }

        // APPROACH #1: Brute force
        {
            Map<String, Map<String, Integer>> output = new HashMap<String, Map<String, Integer>>();
            // start timer
            long duration;
            long startTime = System.currentTimeMillis();
            Iterator<Map.Entry<String, String>> inputIter = input.entrySet().iterator();
            while(inputIter.hasNext()) {
                Map.Entry<String, String> entry = inputIter.next();
                String file = entry.getKey();
                String contents = entry.getValue();

                String[] words = contents.trim().split("\\s+");

                for(String word : words) {

                    Map<String, Integer> files = output.get(word);
                    if (files == null) {
                        files = new HashMap<String, Integer>();
                        output.put(word, files);
                    }

                    Integer occurrences = files.remove(file);
                    if (occurrences == null) {
                        files.put(file, 1);
                    } else {
                        files.put(file, occurrences.intValue() + 1);
                    }
                }
            }
            // get time elapsed
            long endTime = System.currentTimeMillis();
            duration = endTime - startTime;

            // print duration
            System.out.println("Brute Force Duration: " + duration +"ms");
            // show me:
            // System.out.println(output);
        }


        // APPROACH #2: MapReduce
        {
            Map<String, Map<String, Integer>> output = new HashMap<String, Map<String, Integer>>();

            // MAP:

            List<MappedItem> mappedItems = new LinkedList<MappedItem>();

            Iterator<Map.Entry<String, String>> inputIter = input.entrySet().iterator();
            long duration;
            long startTime = System.currentTimeMillis();
            while(inputIter.hasNext()) {
                Map.Entry<String, String> entry = inputIter.next();
                String file = entry.getKey();
                String contents = entry.getValue();

                map(file, contents, mappedItems);
            }
            long endTime = System.currentTimeMillis();
            duration = endTime - startTime;

            // show me:
            System.out.println("Map Reduce - Mapping Phase Duration: " + duration +"ms");

            // GROUP:

            long duration2;
            long startTime2 = System.currentTimeMillis();
            Map<String, List<String>> groupedItems = new HashMap<String, List<String>>();

            Iterator<MappedItem> mappedIter = mappedItems.iterator();
            while(mappedIter.hasNext()) {
                MappedItem item = mappedIter.next();
                String word = item.getWord();
                String file = item.getFile();
                List<String> list = groupedItems.get(word);
                if (list == null) {
                    list = new LinkedList<String>();
                    groupedItems.put(word, list);
                }
                list.add(file);
            }

            long endTime2 = System.currentTimeMillis();
            duration2 = endTime2 - startTime2;

            // show me:
            System.out.println("Map Reduce - Group Phase Duration: " + duration2 +"ms");

            // REDUCE:

            long duration3;
            long startTime3 = System.currentTimeMillis();
            Iterator<Map.Entry<String, List<String>>> groupedIter = groupedItems.entrySet().iterator();
            while(groupedIter.hasNext()) {
                Map.Entry<String, List<String>> entry = groupedIter.next();
                String word = entry.getKey();
                List<String> list = entry.getValue();

                reduce(word, list, output);
            }

            long endTime3 = System.currentTimeMillis();
            duration3 = endTime3 - startTime3;


            System.out.println("Map Reduce - Reduce Phase Duration: " + duration3 +"ms");
            // show me:
            // System.out.println(output);
        }


        // APPROACH #3: Distributed MapReduce
        {
            // Ask the user to enter the number of lines per mapping thread
            System.out.print("Enter number of lines per mapping thread (min 1000, max 10000): ");
            int linesPerThread = scanner.nextInt();
            scanner.nextLine();
            if(linesPerThread < 1000) {
                System.out.print("Invalid number must be (min 1000, max 10000)");
                System.exit(1);
            }
            if (linesPerThread > 10000) {
                System.out.print("Invalid number must be (min 1000, max 10000)");
                System.exit(1);
            }
            final Map<String, Map<String, Integer>> output = new HashMap<String, Map<String, Integer>>();
            final List<MappedItem> mappedItems = new LinkedList<MappedItem>();
            final MapCallback<String, MappedItem> mapCallback = new MapCallback<String, MappedItem>() {
                @Override
                public synchronized void mapDone(String file, List<MappedItem> results) {
                    mappedItems.addAll(results);
                }
            };

            List<Thread> mapCluster = new ArrayList<Thread>(input.size());
            long duration;
            long startTime = System.currentTimeMillis();

            // For each file, read the file as a list of lines and process in chunks.
            for (String file : filePaths) {
                List<String> lines;
                try {
                    lines = readFileLines(file);
                } catch (IOException ex) {
                    System.err.println("Error reading file " + file + ":\n" + ex.getMessage());
                    continue;
                }
                int totalLines = lines.size();
                int start = 0;
                int chunkCount = 0;
                while (start < totalLines) {
                    int end = Math.min(start + linesPerThread, totalLines);
                    // Join the chunk into a single string
                    StringBuilder chunkContent = new StringBuilder();
                    for (int i = start; i < end; i++) {
                        chunkContent.append(lines.get(i)).append(" ");
                    }
                    final String chunkString = chunkContent.toString().trim();
                    // Create an identifier
                    final String chunkId = file + "_" + chunkCount;
                    Thread t = new Thread(new Runnable() {
                        @Override
                        public void run() {
                            map(chunkId, chunkString, mapCallback);
                        }
                    });
                    mapCluster.add(t);
                    t.start();
                    start = end;
                    chunkCount++;
                }
            }

//            while(inputIter.hasNext()) {
//                Map.Entry<String, String> entry = inputIter.next();
//                final String file = entry.getKey();
//                final String contents = entry.getValue();
//
//                Thread t = new Thread(new Runnable() {
//                    @Override
//                    public void run() {
//                        map(file, contents, mapCallback);
//                    }
//                });
//                mapCluster.add(t);
//                t.start();
//            }

            // wait for mapping phase to be over:
            for(Thread t : mapCluster) {
                try {
                    t.join();
                } catch(InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }

            long endTime = System.currentTimeMillis();
            duration = endTime - startTime;
            // show me:
            System.out.println("Distributed Map Reduce - Mapping Phase Duration: " + duration +"ms");


            // GROUP:

            long duration2;
            long startTime2 = System.currentTimeMillis();
            Map<String, List<String>> groupedItems = new HashMap<String, List<String>>();

            Iterator<MappedItem> mappedIter = mappedItems.iterator();
            while(mappedIter.hasNext()) {
                MappedItem item = mappedIter.next();
                String word = item.getWord();
                String file = item.getFile();
                List<String> list = groupedItems.get(word);
                if (list == null) {
                    list = new LinkedList<String>();
                    groupedItems.put(word, list);
                }
                list.add(file);
            }

            long endTime2 = System.currentTimeMillis();
            duration2 = endTime2 - startTime2;

            // show me:
            System.out.println("Distributed Map Reduce - Grouping Phase Duration: " + duration2 +"ms");

            // REDUCE:
            long duration3;
            long startTime3 = System.currentTimeMillis();
            final ReduceCallback<String, String, Integer> reduceCallback = new ReduceCallback<String, String, Integer>() {
                @Override
                public synchronized void reduceDone(String k, Map<String, Integer> v) {
                    output.put(k, v);
                }
            };


            List<Map.Entry<String, List<String>>> entries = new ArrayList<>(groupedItems.entrySet());
            List<Thread> reduceCluster = new ArrayList<>();


            System.out.print("Enter number of words per thread (min 1000, max 10000): ");
            int wordsPerThread = scanner.nextInt();
            scanner.nextLine();
            if(wordsPerThread < 1000) {
                System.out.print("Invalid number must be (min 1000, max 10000)");
                System.exit(1);
            }
            if (wordsPerThread > 10000) {
                System.out.print("Invalid number must be (min 1000, max 10000)");
                System.exit(1);
            }

            int totalEntries = entries.size();
            int index = 0;
            while (index < totalEntries) {
                // Tentatively set the chunk end to either the maximum allowed or the total size.
                int chunkEnd = Math.min(index + wordsPerThread, totalEntries);
                // If this is not the first chunk and the remaining words are less than the minimum (100),
                // then combine them with the previous chunk.
                if (chunkEnd - index < 100 && index != 0) {
                    chunkEnd = totalEntries;
                }
                // Create a sublist (chunk) for this thread.
                List<Map.Entry<String, List<String>>> subEntries = entries.subList(index, chunkEnd);

                Thread t = new Thread(new Runnable() {
                    @Override
                    public void run() {
                        // For each word in this chunk, call reduce
                        for (Map.Entry<String, List<String>> entry : subEntries) {
                            reduce(entry.getKey(), entry.getValue(), reduceCallback);
                        }
                    }
                });
                reduceCluster.add(t);
                t.start();

                index = chunkEnd;
            }


//            List<Thread> reduceCluster = new ArrayList<Thread>(groupedItems.size());

//            Iterator<Map.Entry<String, List<String>>> groupedIter = groupedItems.entrySet().iterator();
//            while(groupedIter.hasNext()) {
//                Map.Entry<String, List<String>> entry = groupedIter.next();
//                final String word = entry.getKey();
//                final List<String> list = entry.getValue();
//
//                Thread t = new Thread(new Runnable() {
//                    @Override
//                    public void run() {
//                        reduce(word, list, reduceCallback);
//                    }
//                });
//                reduceCluster.add(t);
//                t.start();
//            }

            // wait for reducing phase to be over:
            for(Thread t : reduceCluster) {
                try {
                    t.join();
                } catch(InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }

            long endTime3 = System.currentTimeMillis();
            duration3 = endTime3 - startTime3;

            // show me:
            System.out.println("Distributed MapReduce Reduce Duration: " + duration3 +"ms");

//            System.out.println(output);
        }
    }


    public static void map(String file, String contents, List<MappedItem> mappedItems) {
        String[] words = contents.trim().split("\\s+");
        for(String word: words) {
            mappedItems.add(new MappedItem(word, file));
        }
    }

    public static void reduce(String word, List<String> list, Map<String, Map<String, Integer>> output) {
        Map<String, Integer> reducedList = new HashMap<String, Integer>();
        for(String file: list) {
            Integer occurrences = reducedList.get(file);
            if (occurrences == null) {
                reducedList.put(file, 1);
            } else {
                reducedList.put(file, occurrences.intValue() + 1);
            }
        }
        output.put(word, reducedList);
    }

    public static interface MapCallback<E, V> {

        public void mapDone(E key, List<V> values);
    }

    public static void map(String file, String contents, MapCallback<String, MappedItem> callback) {
        String[] words = contents.trim().split("\\s+");
        List<MappedItem> results = new ArrayList<MappedItem>(words.length);
        for(String word: words) {
            // Change the word to its pure form
            String pureWord = word.replaceAll("[^a-zA-Z]", "");
            results.add(new MappedItem(pureWord, file));
        }
        callback.mapDone(file, results);
    }

    public static interface ReduceCallback<E, K, V> {

        public void reduceDone(E e, Map<K,V> results);
    }

    public static void reduce(String word, List<String> list, ReduceCallback<String, String, Integer> callback) {

        Map<String, Integer> reducedList = new HashMap<String, Integer>();
        for(String file: list) {
            Integer occurrences = reducedList.get(file);
            if (occurrences == null) {
                reducedList.put(file, 1);
            } else {
                reducedList.put(file, occurrences.intValue() + 1);
            }
        }
        callback.reduceDone(word, reducedList);
    }

    private static class MappedItem {

        private final String word;
        private final String file;

        public MappedItem(String word, String file) {
            this.word = word;
            this.file = file;
        }

        public String getWord() {
            return word;
        }

        public String getFile() {
            return file;
        }

        @Override
        public String toString() {
            return "[\"" + word + "\",\"" + file + "\"]";
        }
    }

    // private method to read in the lines of the File
    private static List<String> readFileLines(String pathname) throws IOException {
        List<String> lines = new ArrayList<>();
        BufferedReader br = new BufferedReader(new FileReader(pathname));
        String line;
        while ((line = br.readLine()) != null) {
            if(line.length() > 80) {
                lines.addAll(splitLine(line));
            } else {
                lines.add(line);
            }
        }
        br.close();
        return lines;
    }

    // breaking the line into length of roughly 80 characters without breaking up the words and then preprending the rest of the line to the next line
    private static List<String> splitLine(String line){
        List<String> parts = new ArrayList<>();
        int maxLen = 80;
        String remaining = line;

        while (remaining.length() > maxLen) {
            int splitIndex = remaining.lastIndexOf(" ", maxLen);
            if (splitIndex == -1) {
                splitIndex = maxLen;
            }
            String part = remaining.substring(0, splitIndex).trim();
            parts.add(part);
            remaining = remaining.substring(splitIndex).trim();
        }
        if(!remaining.isEmpty()) {
            parts.add(remaining);
        }
        return parts;
    }

    private static String readFile(String pathname) throws IOException {
        File file = new File(pathname);
        StringBuilder fileContents = new StringBuilder((int) file.length());
        Scanner scanner = new Scanner(new BufferedReader(new FileReader(file)));
        String lineSeparator = System.getProperty("line.separator");

        try {
            if (scanner.hasNextLine()) {
                fileContents.append(scanner.nextLine());
            }
            while (scanner.hasNextLine()) {
                fileContents.append(lineSeparator + scanner.nextLine());
            }
            return fileContents.toString();
        } finally {
            scanner.close();
        }
    }

}