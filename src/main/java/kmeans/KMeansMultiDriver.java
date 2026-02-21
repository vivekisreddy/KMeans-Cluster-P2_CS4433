package kmeans;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.net.URI;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.List;
import java.util.ArrayList;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class KMeansMultiDriver {

    public static void main(String[] args) throws Exception {
        if (args.length < 4) {
            System.err.println("Usage: KMeansMultiDriver <points input> <initial centroids> <output dir> <R iterations>");
            System.exit(-1);
        }

        String pointsInput = args[0];
        String centroidsInput = args[1];
        String outputDir = args[2];
        int R = Integer.parseInt(args[3]);
        double convergenceThreshold = 0.001; // optional threshold for early stop

        String currentCentroids = centroidsInput;
        List<Centroid> oldCentroids = null;

        FileSystem fs = FileSystem.get(new Configuration());

        for (int i = 1; i <= R; i++) {
            Configuration conf = new Configuration();
            Job job = Job.getInstance(conf, "KMeans Iteration " + i);

            job.setJarByClass(KMeansMultiDriver.class);
            job.setMapperClass(KMeansMapper.class);
            job.setReducerClass(KMeansReducer.class);
            job.setCombinerClass(KMeansCombiner.class);

            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(Text.class);

            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);

            // Add centroids to distributed cache
            job.addCacheFile(new URI(currentCentroids + "#centroids.txt"));

            FileInputFormat.addInputPath(job, new Path(pointsInput));
            String iterOutput = outputDir + "/iter" + i;
            FileOutputFormat.setOutputPath(job, new Path(iterOutput));

            boolean success = job.waitForCompletion(true);
            if (!success) {
                System.err.println("Iteration " + i + " failed!");
                System.exit(1);
            }

            // Early convergence check
            List<Centroid> newCentroids = readCentroids(fs, iterOutput + "/part-r-00000");
            if (oldCentroids != null) {
                double maxDistance = 0.0;
                for (int j = 0; j < newCentroids.size(); j++) {
                    double dist = Centroid.distance(oldCentroids.get(j), newCentroids.get(j));
                    if (dist > maxDistance) maxDistance = dist;
                }
                if (maxDistance < convergenceThreshold) {
                    System.out.println("Converged after iteration " + i);
                    break;
                }
            }
            oldCentroids = newCentroids;
            currentCentroids = iterOutput + "/part-r-00000";
        }

        System.out.println("KMeans completed.");
    }

    private static List<Centroid> readCentroids(FileSystem fs, String pathStr) throws Exception {
        List<Centroid> centroids = new ArrayList<>();
        Path path = new Path(pathStr);
        BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(path)));
        String line;
        while ((line = br.readLine()) != null) {
            String[] parts = line.trim().split(",");
            centroids.add(new Centroid(Double.parseDouble(parts[0]), Double.parseDouble(parts[1])));
        }
        br.close();
        return centroids;
    }
}
