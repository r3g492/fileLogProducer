package pipeline;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.io.File;
import java.util.Properties;
import java.util.Scanner;

@SpringBootApplication
public class GwProducer {

    private final static String TOPIC = "hello";
    private final static String BOOTSTRAP_SERVERS = "your_IP:9092";

    public static void main(String[] args) {

        Properties properties = new Properties();
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);

        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(properties);

        Scanner scanner = new Scanner(System.in);
        System.out.println("cliFileLogger!!");
        while (true) {
            System.out.print("Work available! Please input your folder path. (exit cmd : exit) : ");
            String folderPath = scanner.nextLine();
            if (folderPath.equals("exit")) {
                break;
            }
            Validator validator = new Validator();
            File folder = new File(folderPath);
            if (!validator.isValid(folder)) {
                System.out.println("folder path is wrong, back to inputting path.");
            } else {
                System.out.println("inputted folder path : " + folderPath);
                System.out.print("log file is saved on Working Directory \n");

                // workingPath 등록
                String workingPath = System.getProperty("user.dir");
                //String tmpWorkingPath = workingPath.replace("\\", "/");
                System.out.println("Working Directory = " + workingPath);

                //
                Logger logger = new Logger(workingPath, folderPath);
                logger.log(kafkaProducer, TOPIC);
                //break;
            }
        }

        kafkaProducer.flush();
        kafkaProducer.close();

    }
}
class Validator {
    public boolean isValid(File folder) {
        if (folder.exists()) {
            return folder.isDirectory();
        }
        return false;
    }
}

