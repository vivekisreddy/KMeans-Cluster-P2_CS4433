package kmeans;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;

public class KMeansCombiner extends Reducer<Text, Text, Text, Text> {

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {

        double sumX = 0.0, sumY = 0.0;
        int count = 0;

        for (Text val : values) {
            String[] parts = val.toString().split(",");
            double x = Double.parseDouble(parts[0]);
            double y = Double.parseDouble(parts[1]);
            int c = Integer.parseInt(parts[2]);
            sumX += x;
            sumY += y;
            count += c;
        }

        // Emit partial sum and count
        context.write(key, new Text(sumX + "," + sumY + "," + count));
    }
}
