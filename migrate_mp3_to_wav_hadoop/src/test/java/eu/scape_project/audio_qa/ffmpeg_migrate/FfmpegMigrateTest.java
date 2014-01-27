package eu.scape_project.audio_qa.ffmpeg_migrate;

import eu.scape_project.audio_qa.AudioQASettings;
import eu.scape_project.audio_qa.MigrateMp3ToWav;
import eu.scape_project.audio_qa.MigrationMapper;
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
 * eu.scape_project.audio_qa.ffmpeg_migrate
 * User: baj@statsbiblioteket.dk
 * Date: 2014-01-15
 */
public class FfmpegMigrateTest {
    MapReduceDriver<LongWritable, Text, LongWritable, Text, LongWritable, Text> mapReduceDriver;
    MapDriver<LongWritable, Text, LongWritable, Text> mapDriver;
    ReduceDriver<LongWritable, Text, LongWritable, Text> reduceDriver;

    Text inputFilePath = new Text("/home/bolette/Projects/scape-audio-qa/migrate_mp3_to_wav_workflow/src/main/samples/freestylemix_-_hisboyelroy_-_Revolve.mp3");
    Text outputdir = new Text("output/MigrateMp3ToWav/freestylemix_-_hisboyelroy_-_Revolve");

    @Before
    public void setUp() {
        AudioQASettings.MAPPER_OUTPUT_DIR = "output/MigrateMp3ToWav/";
        FfmpegMigrationMapper mapper = new FfmpegMigrationMapper();
        FfmpegMigrate.FfmpegMigrationReducer reducer = new FfmpegMigrate.FfmpegMigrationReducer();
        mapDriver = new MapDriver<LongWritable, Text, LongWritable, Text>();
        mapDriver.setMapper(mapper);
        reduceDriver = new ReduceDriver<LongWritable, Text, LongWritable, Text>();
        reduceDriver.setReducer(reducer);
        mapReduceDriver = new MapReduceDriver<LongWritable, Text, LongWritable, Text, LongWritable, Text>();
        mapReduceDriver.setMapper(mapper);
        mapReduceDriver.setReducer(reducer);
    }

    @Test
    public void testMapper() throws IOException {
        mapDriver.withInput(new LongWritable(0), inputFilePath);
        mapDriver.withOutput(new LongWritable(0), outputdir);
        mapDriver.runTest();
    }

    @Test
    public void testReducer() throws IOException {
        List<Text> values = new ArrayList<Text>();
        values.add(outputdir);
        //TODO test multiple values
        reduceDriver.withInput(new LongWritable(0), values);
        reduceDriver.withOutput(new LongWritable(0), new Text(outputdir.toString()+"\n"));
        reduceDriver.runTest();
    }

    @Test
    public void testMapReduce() throws IOException {
        mapReduceDriver.withInput(new LongWritable(0), inputFilePath);
        mapReduceDriver.addOutput(new LongWritable(0), new Text(outputdir.toString()+"\n"));
        mapReduceDriver.runTest();
    }
}
