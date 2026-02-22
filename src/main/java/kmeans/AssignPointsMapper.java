package kmeans;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.*;
import java.util.*;

public class AssignPointsMapper extends Mapper<LongWritable, Text, Text, Text> {

    private List<Centroid> centroids = new ArrayList<>();

    @Override
    protected void setup(Context context) throws IOException {
        BufferedReader br = new BufferedReader(new FileReader("final_centroids.txt"));
        String line;
        while ((line = br.readLine()) != null) {
            if (line.startsWith("Converged?")) continue; // skip convergence line
            String[] parts = line.trim().split(",");
            centroids.add(new Centroid(
                    Double.parseDouble(parts[0]),
                    Double.parseDouble(parts[1])
            ));
        }
        br.close();
    }

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        String[] parts = value.toString().trim().split(",");
        double px = Double.parseDouble(parts[0]);
        double py = Double.parseDouble(parts[1]);

        double minDistance = Double.MAX_VALUE;
        Centroid closestCentroid = null;

        for (Centroid c : centroids) {
            double dist = c.distance(px, py);
            if (dist < minDistance) {
                minDistance = dist;
                closestCentroid = c;
            }
        }

        // Output: centroid -> point
        context.write(new Text(closestCentroid.toString()), new Text(px + "," + py));
    }
}