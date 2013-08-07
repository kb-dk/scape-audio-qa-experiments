package eu.scape_project.audio_qa;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.Tool;

import java.io.IOException;

/**
 * Create map-reduce job for QA of the migrated wav files on the given input list.
 * eu.scape_project
 * User: baj@statsbiblioteket.dk
 * Date: 8/7/13
 * Time: 10:26 AM
 */
public class QAMigratedWav extends Configured implements Tool {

    static public class QAReducer extends Reducer<Text, LongWritable, Text, LongWritable> {
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

    @Override
    public int run(String[] strings) throws Exception {
        return 0;  //To change body of implemented methods use File | Settings | File Templates.
    }
}
