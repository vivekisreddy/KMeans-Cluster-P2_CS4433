package kmeans;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;

public class KMeansReducer extends Reducer<Text, Text, Text, Text> {

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {

        double sumX = 0.0, sumY = 0.0;
        int count = 0;

        for (Text val : values) {
            String[] parts = val.toString().split(",");
            double xSum = Double.parseDouble(parts[0]);
            double ySum = Double.parseDouble(parts[1]);
            int c = Integer.parseInt(parts[2]);
            sumX += xSum;
            sumY += ySum;
            count += c;
        }

        double newX = sumX / count;
        double newY = sumY / count;

        context.write(new Text(newX + "," + newY), new Text(""));
    }
}
