package main.java.eu.scape_project.audio_qa;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

/**
 * eu.scape_project
 * User: bam
 * Date: 6/28/13
 * Time: 10:22 AM
 */
public class MigrateMp3ToWav extends Configured implements Tool {

    static public class MigrationMapper extends Mapper<LongWritable, Text, Text, LongWritable> {
        //populate the input/output files into sensible variable names
        private String[] inFiles = null;
        private String tempDir = "";
        private String outFile = "";
        private String logFile = "";

        @Override
        protected void map(LongWritable lineNo , Text inputMp3path, Context context) throws IOException, InterruptedException {
            //start with ffprobe
            List<String> commandLine = new ArrayList<String>();
            commandLine.add("ffprobe");
            commandLine.add(inputMp3path.toString());
            ProcessBuilder pb = new ProcessBuilder("ffprobe", inputMp3path.toString());

            //start the executable
            Process proc = pb.start();
            BufferedReader stdout = new BufferedReader(new InputStreamReader(proc.getInputStream()));
            BufferedReader stderr = new BufferedReader(new InputStreamReader(proc.getErrorStream()));
            try {
                //wait for process to end before continuing
                proc.waitFor();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            int exitCode = proc.exitValue();
            String stdoutString = stdout.ready() ? stdout.readLine() : "";
            String stderrString = stderr.ready() ? stderr.readLine() : "";
            //write command + log of stdout and stderr to the output key text
            Text output = new Text("ffprobe\n" + stdoutString + "\n" + stderrString + "\n");

            //TODO migrate

            context.write(output, new LongWritable(exitCode));
        }
    }

    static public class MigrationReducer extends Reducer<Text, LongWritable, Text, LongWritable> {
        private LongWritable total = new LongWritable();

        @Override
        protected void reduce(Text output, Iterable<LongWritable> exitCodes, Context context)
                throws IOException, InterruptedException {
            long n = 0;
            for (LongWritable count : exitCodes)
                n += count.get();
            total.set(n);
            context.write(output, total);
        }
    }

    public int run(String[] args) throws Exception {
        Configuration configuration = getConf();

        Job job = new Job(configuration, "Migrate Mp3 To Wav");
        job.setJarByClass(MigrateMp3ToWav.class);

        int n = args.length;
        if (n > 0)
            TextInputFormat.addInputPath(job, new Path(args[0]));
        if (n > 1)
            SequenceFileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setMapperClass(MigrationMapper.class);
        job.setCombinerClass(MigrationReducer.class);
        job.setReducerClass(MigrationReducer.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        return job.waitForCompletion(true) ? 0 : -1;
    }

    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new MigrateMp3ToWav(), args));
    }
}
