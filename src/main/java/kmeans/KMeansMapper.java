package kmeans;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.fs.*;
import java.io.*;
import java.net.URI;
import java.util.*;

public class KMeansMapper extends Mapper<LongWritable, Text, Text, Text> {

    private List<Centroid> centroids = new ArrayList<>();

    @Override
    protected void setup(Context context) throws IOException {
        URI[] cacheFiles = context.getCacheFiles();
        if (cacheFiles == null || cacheFiles.length == 0) {
            throw new FileNotFoundException("Centroids file missing in distributed cache!");
        }

        // Read centroids from distributed cache
        Path centroidPath = new Path("centroids.txt");
        FileSystem fs = FileSystem.get(context.getConfiguration());
        BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(centroidPath)));
        String line;
        while ((line = br.readLine()) != null) {
            String[] parts = line.trim().split(",");
            centroids.add(new Centroid(Double.parseDouble(parts[0]), Double.parseDouble(parts[1])));
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

        // Emit centroid key with point and count = 1
        context.write(new Text(closestCentroid.toString()), new Text(px + "," + py + ",1"));
    }
}
