package eu.scape_project.audio_qa.ffmpeg_migrate;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

/**
 * Create the map-reduce job for migrating the mp3 files on the given input list using ffmpeg.
 * eu.scape_project.audio_qa.ffmpeg_migrate
 * User: baj@statsbiblioteket.dk
 * Date: 2014-01-14
 */
public class FfmpegMigrate  extends Configured implements Tool {
    static public class FfmpegMigrationReducer extends Reducer<LongWritable, Text, LongWritable, Text> {

        @Override
        protected void reduce(LongWritable exitCode, Iterable<Text> outputs, Context context)
                throws IOException, InterruptedException {
            Text list = new Text("");
            for (Text output : outputs) {
                list = new Text(list.toString() + output.toString() + "\n");
            }
            context.write(exitCode, list);
        }
    }

    public int run(String[] args) throws Exception {
        Configuration configuration = getConf();

        Job job = Job.getInstance(configuration);
        job.setJarByClass(FfmpegMigrate.class);

        int n = args.length;
        if (n > 0)
            TextInputFormat.addInputPath(job, new Path(args[0]));
        if (n > 1)
            SequenceFileOutputFormat.setOutputPath(job, new Path(args[1]));
        if (n > 2)
            configuration.set("map.outputdir", args[2]);
        if (n > 3)
            configuration.set("tool.outputdir", args[3]);

        job.setMapperClass(FfmpegMigrationMapper.class);
        job.setReducerClass(FfmpegMigrationReducer.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(Text.class);

        job.setNumReduceTasks(1);

        if (job.getJobID()==null) {configuration.set("job.jobID", "FfmpegMigrate" + Math.random());}

        return job.waitForCompletion(true) ? 0 : -1;
    }

    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new FfmpegMigrate(), args));
    }
}
