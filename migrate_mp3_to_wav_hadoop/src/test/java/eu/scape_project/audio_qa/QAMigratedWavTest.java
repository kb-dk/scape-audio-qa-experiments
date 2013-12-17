package eu.scape_project.audio_qa;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * eu.scape_project.audio_qa
 * User: baj@statsbiblioteket.dk
 * Date: 12/10/13
 * Time: 11:32 AM
 * See the <a href="https://cwiki.apache.org/confluence/display/MRUNIT/Index">MRUnit Wiki</a> for more information.
 */
public class QAMigratedWavTest {
    MapReduceDriver<LongWritable, Text, LongWritable, Text, LongWritable, Text> mapReduceDriver;
    MapDriver<LongWritable, Text, LongWritable, Text> mapDriver;
    ReduceDriver<LongWritable, Text, LongWritable, Text> reduceDriver;

    Text inputFilePath = new Text("/home/bam/Projects/scape-audio-qa/migrate_mp3_to_wav_workflow/src/main/samples/freestylemix_-_hisboyelroy_-_Revolve.mp3");
    Text outputdir = new Text("output/MigrateMp3ToWav/freestylemix_-_hisboyelroy_-_Revolve");
    Text qaOutput = new Text("outputDir: output/MigrateMp3ToWav/freestylemix_-_hisboyelroy_-_Revolve\n" +
            "inputMp3: freestylemix_-_hisboyelroy_-_Revolve.mp3");

    @Before
    public void setUp() {
        AudioQASettings.OUTPUT_DIR = "output/MigrateMp3ToWav/";
        QAMapper mapper = new QAMapper();
        //QAMigratedWav.QAReducer reducer = new QAMigratedWav.QAReducer();
        mapDriver = new MapDriver<LongWritable, Text, LongWritable, Text>();
        mapDriver.setMapper(mapper);
        reduceDriver = new ReduceDriver<LongWritable, Text, LongWritable, Text>();
        //reduceDriver.setReducer(reducer);
        mapReduceDriver = new MapReduceDriver<LongWritable, Text, LongWritable, Text, LongWritable, Text>();
        mapReduceDriver.setMapper(mapper);
        //mapReduceDriver.setReducer(reducer);
    }

    @Test
    public void testMapper() throws IOException {
        mapDriver.withInput(new LongWritable(0), inputFilePath);
        mapDriver.withOutput(new LongWritable(0), outputdir);
        mapDriver.runTest();
    }

    /* TODO update tests to chainmapper
    @Test
    public void testReducer() throws IOException {
        List<LongWritable> values = new ArrayList<LongWritable>();
        values.add(new LongWritable(0));
        values.add(new LongWritable(0));
        reduceDriver.withInput(outputdir, values);
        reduceDriver.withOutput(qaOutput, new LongWritable(0));
        reduceDriver.runTest();
    }

    @Test
    public void testMapReduce() throws IOException {
        mapReduceDriver.withInput(new LongWritable(0), inputFilePath);
        mapReduceDriver.addOutput(qaOutput, new LongWritable(0));
        mapReduceDriver.runTest();
    }
    */
}
