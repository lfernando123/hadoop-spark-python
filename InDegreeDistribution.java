import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class InDegreeDistribution {

    // ========================================================================
    // Job 1: Calculate In-Degree for each node
    // Input: (source, destination)
    // Output: (destination, in_degree)
    // ========================================================================

    public static class InDegreeMapper extends Mapper<Object, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        private Text destinationNode = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            // Skip comment lines starting with '#'
            if (line.startsWith("#")) {
                return;
            }

            StringTokenizer tokenizer = new StringTokenizer(line);
            if (tokenizer.countTokens() >= 2) {
                // The first token is the source, the second is the destination
                tokenizer.nextToken(); // Skip source node
                destinationNode.set(tokenizer.nextToken()); // Get destination node
                context.write(destinationNode, one);
            }
        }
    }

    public static class InDegreeReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            // Output: (node_id, in_degree)
            context.write(key, result);
        }
    }

    // ========================================================================
    // Job 2: Calculate Distribution (Count of nodes per In-Degree)
    // Input: (node_id, in_degree)
    // Output: (in_degree, count_of_nodes)
    // ========================================================================

    public static class DistributionMapper extends Mapper<Object, Text, IntWritable, IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        private IntWritable inDegree = new IntWritable();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] parts = line.split("\\s+"); // Split by whitespace (tab or space)

            if (parts.length >= 2) {
                try {
                    // The second part is the in-degree from Job 1 output
                    int degree = Integer.parseInt(parts[1]);
                    inDegree.set(degree);
                    // Output: (in_degree, 1)
                    context.write(inDegree, one);
                } catch (NumberFormatException e) {
                    // Handle potential parsing errors if the input is malformed
                    System.err.println("Skipping malformed line: " + line);
                }
            }
        }
    }

    public static class DistributionReducer extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {
        private IntWritable result = new IntWritable();

        public void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            // Output: (in_degree, count_of_nodes)
            context.write(key, result);
        }
    }

    // ========================================================================
    // Main Driver
    // ========================================================================

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length != 3) {
            System.err.println("Usage: InDegreeDistribution <input_path> <temp_path> <output_path>");
            System.exit(2);
        }

        Path inputPath = new Path(otherArgs[0]);
        Path tempPath = new Path(otherArgs[1]);
        Path outputPath = new Path(otherArgs[2]);

        // --------------------------------------------------------------------
        // Job 1: Calculate In-Degree
        // --------------------------------------------------------------------
        Job job1 = Job.getInstance(conf, "In-Degree Calculation");
        job1.setJarByClass(InDegreeDistribution.class);
        job1.setMapperClass(InDegreeMapper.class);
        job1.setCombinerClass(InDegreeReducer.class); // Use Reducer as Combiner for efficiency
        job1.setReducerClass(InDegreeReducer.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job1, inputPath);
        FileOutputFormat.setOutputPath(job1, tempPath);

        if (!job1.waitForCompletion(true)) {
            System.exit(1);
        }

        // --------------------------------------------------------------------
        // Job 2: Calculate Distribution
        // --------------------------------------------------------------------
        Job job2 = Job.getInstance(conf, "In-Degree Distribution");
        job2.setJarByClass(InDegreeDistribution.class);
        job2.setMapperClass(DistributionMapper.class);
        job2.setCombinerClass(DistributionReducer.class); // Use Reducer as Combiner for efficiency
        job2.setReducerClass(DistributionReducer.class);
        job2.setOutputKeyClass(IntWritable.class);
        job2.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job2, tempPath);
        FileOutputFormat.setOutputPath(job2, outputPath);

        System.exit(job2.waitForCompletion(true) ? 0 : 1);
    }
}
