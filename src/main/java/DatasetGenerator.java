import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Random;

public class DatasetGenerator {

    public static void main(String[] args) throws IOException {

        if (args.length != 3) {
            System.out.println("Usage: java DatasetGenerator <numPoints> <K> <outputPrefix>");
            System.exit(1);
        }

        int numPoints = Integer.parseInt(args[0]);
        int K = Integer.parseInt(args[1]);
        String prefix = args[2];

        Random rand = new Random();

        // Generate Large Dataset
        BufferedWriter dataWriter = new BufferedWriter(
                new FileWriter(prefix + "_points.txt"));

        for (int i = 0; i < numPoints; i++) {
            int x = rand.nextInt(5001); // 0–5000
            int y = rand.nextInt(5001);
            dataWriter.write(x + "," + y);
            dataWriter.newLine();
        }

        dataWriter.close();

        // Generate Initial Centroids
        BufferedWriter centroidWriter = new BufferedWriter(
                new FileWriter(prefix + "_centroids.txt"));

        for (int i = 0; i < K; i++) {
            int x = rand.nextInt(10001); // 0–10000
            int y = rand.nextInt(10001);
            centroidWriter.write(x + "," + y);
            centroidWriter.newLine();
        }

        centroidWriter.close();

        System.out.println("Dataset and centroids generated successfully.");
    }
}
