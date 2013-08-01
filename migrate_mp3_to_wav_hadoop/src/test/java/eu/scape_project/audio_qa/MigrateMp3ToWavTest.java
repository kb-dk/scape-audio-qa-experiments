package eu.scape_project.audio_qa;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

/**
 * eu.scape_project
 * User: bam
 * Date: 6/28/13
 * Time: 11:00 AM
 * See the <a href="https://cwiki.apache.org/confluence/display/MRUNIT/Index">MRUnit Wiki</a> for more information.
 */
public class MigrateMp3ToWavTest {
    MapReduceDriver<LongWritable, Text, Text, LongWritable, Text, LongWritable> mapReduceDriver;
    MapDriver<LongWritable, Text, Text, LongWritable> mapDriver;
    ReduceDriver<Text, LongWritable, Text, LongWritable> reduceDriver;

    Text inputFilePath = new Text("/home/bam/Projects/scape-audio-qa/migrate_mp3_to_wav_workflow/src/main/samples/freestylemix_-_hisboyelroy_-_Revolve.mp3");
    Text outputdir = new Text("test-output/MigrateMp3ToWav/freestylemix_-_hisboyelroy_-_Revolve");

    @Before
    public void setUp() {
        MigrationMapper mapper = new MigrationMapper();
        MigrateMp3ToWav.MigrationReducer reducer = new MigrateMp3ToWav.MigrationReducer();
        mapDriver = new MapDriver<LongWritable, Text, Text, LongWritable>();
        mapDriver.setMapper(mapper);
        reduceDriver = new ReduceDriver<Text, LongWritable, Text, LongWritable>();
        reduceDriver.setReducer(reducer);
        mapReduceDriver = new MapReduceDriver<LongWritable, Text, Text, LongWritable, Text, LongWritable>();
        mapReduceDriver.setMapper(mapper);
        mapReduceDriver.setReducer(reducer);
    }

    @Test
    public void testMapper() {
        mapDriver.withInput(new LongWritable(0), inputFilePath);
        mapDriver.withOutput(outputdir, new LongWritable(0));
        mapDriver.runTest();
    }

    @Test
    public void testReducer() {
        List<LongWritable> values = new ArrayList<LongWritable>();
        values.add(new LongWritable(0));
        values.add(new LongWritable(0));
        reduceDriver.withInput(outputdir, values);
        reduceDriver.withOutput(outputdir, new LongWritable(0));
        reduceDriver.runTest();
    }

    @Test
    public void testMapReduce() {
        mapReduceDriver.withInput(new LongWritable(0), inputFilePath);
        mapReduceDriver.addOutput(outputdir, new LongWritable(0));
        mapReduceDriver.runTest();
    }
}
