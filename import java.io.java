import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WordCount {

    // Mapper class
    public static class TokenizerMapper
            extends Mapper<Object, org.apache.hadoop.io.Text, IntWritable, IntWritable> {

        private final static IntWritable one = new IntWritable(1);
        private IntWritable wordLength = new IntWritable();

        public void map(Object key, org.apache.hadoop.io.Text value, Context context)
                throws IOException, InterruptedException {

            StringTokenizer itr = new StringTokenizer(value.toString());
            System.out.println("Mapper Output:");
            while (itr.hasMoreTokens()) {
                String word = itr.nextToken();
                wordLength.set(word.length());
                System.out.println("(" + wordLength.get() + ", 1)");
                context.write(wordLength, one);
            }
        }
    }

    // Reducer class
    public static class IntSumReducer
            extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {

        private boolean printedHeader = false;

        public void reduce(IntWritable key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {

            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }

            if (!printedHeader) {
                System.out.println("\nReducer Output:");
                printedHeader = true;
            }

            System.out.println("(" + key.get() + ", " + sum + ")");
            context.write(key, new IntWritable(sum));
        }
    }

    // Driver code
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        // Run locally so Mapper/Reducer output appears in console
        conf.set("mapreduce.framework.name", "local");

        Job job = Job.getInstance(conf, "word length count");
        job.setJarByClass(WordCount.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);

        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
