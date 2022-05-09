package pipeline;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Properties;

public class Logger {
    FileWriter fw;
    String txt;
    String folderPath;
    int fileCnt = 0;
    int errorCnt = 0;
    int logID = 0;
    Logger(String workingPath, String folderPath) {

        try {
            this.folderPath = folderPath;
            File file = new File(workingPath + "\\log" + logID + ".txt");
            while (true) {
                if (file.exists()) {
                    logID++;
                    txt = workingPath + "\\log" + logID + ".txt";
                    file = new File(txt);
                } else {
                    txt = workingPath + "\\log" + logID + ".txt";
                    break;
                }
            }
        } catch (Exception e) {
            System.out.println("Logger init fail.");
            e.printStackTrace();
        }

    }
    public void log() {
        long beforeTime = System.currentTimeMillis();
        try {
            // show working status
            System.out.println("working ...");

            File write = new File(txt);
            fw = new FileWriter(write);
            Files.find(Paths.get(folderPath),
                    Integer.MAX_VALUE,
                    (p, bfa) -> !bfa.isDirectory())
                    .forEach(s -> {
                try {
                    String writeString = s.toString() + ":" + s.getFileName() + "\n";
                    fw.write(writeString);

                    //

                    fileCnt++;
                } catch (IOException e) {
                    errorCnt++;
                    System.out.println("FileWriter error");
                    e.printStackTrace();
                }
            });
            long afterTime = System.currentTimeMillis();
            long secDiffTime = (afterTime - beforeTime);
            fw.write("\n" + "Folder Path : " + folderPath + "\n");
            fw.write("File Count : " + fileCnt + "\n");
            fw.write("Time Spent : " + secDiffTime + "ms" + "\n");
            fw.write("Error Count : " + errorCnt + "\n");
            fw.flush();
            fw.close();
            System.out.println("Folder Path : " + folderPath);
            System.out.println("File Count : " + fileCnt);
            System.out.println("Time Spent : " + secDiffTime + "ms");
            System.out.println("Error Count : " + errorCnt);
        } catch (Exception e) {
            errorCnt++;
            System.out.println("Log fail.");
            e.printStackTrace();
        }
    }
    public void log(KafkaProducer<String, String> kafkaProducer, String topic) {
        long beforeTime = System.currentTimeMillis();
        try {
            // show working status
            System.out.println("working ...");

            File write = new File(txt);
            fw = new FileWriter(write);
            Files.find(Paths.get(folderPath),
                            Integer.MAX_VALUE,
                            (p, bfa) -> !bfa.isDirectory())
                    .forEach(s -> {
                        try {
                            String writeString = s.toString() + ":" + s.getFileName() + "\n";
                            fw.write(writeString);

                            ProducerRecord<String, String> record = new ProducerRecord<>(topic, writeString);
                            kafkaProducer.send(record);

                            fileCnt++;
                        } catch (IOException e) {
                            errorCnt++;
                            System.out.println("FileWriter error");
                            e.printStackTrace();
                        }
                    });
            long afterTime = System.currentTimeMillis();
            long secDiffTime = (afterTime - beforeTime);
            fw.write("\n" + "Folder Path : " + folderPath + "\n");
            fw.write("File Count : " + fileCnt + "\n");
            fw.write("Time Spent : " + secDiffTime + "ms" + "\n");
            fw.write("Error Count : " + errorCnt + "\n");
            fw.flush();
            fw.close();
            System.out.println("Folder Path : " + folderPath);
            System.out.println("File Count : " + fileCnt);
            System.out.println("Time Spent : " + secDiffTime + "ms");
            System.out.println("Error Count : " + errorCnt);
        } catch (Exception e) {
            errorCnt++;
            System.out.println("Log fail.");
            e.printStackTrace();
        }
    }
}
