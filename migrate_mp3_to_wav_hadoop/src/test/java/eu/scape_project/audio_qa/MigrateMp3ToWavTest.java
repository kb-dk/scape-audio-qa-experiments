package eu.scape_project.audio_qa;

import junit.framework.TestCase;
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

    Text inputFilePath = new Text("/home/bam/Projects/scape-audio-qa/src/test/resources/thermo.wav");
    Text output = new Text("ffprobe\n" +
            "ffprobe version 0.10.3 Copyright (c) 2007-2012 the FFmpeg developers\n" +
            "  built on May 23 2012 10:22:41 with gcc 4.6.3 20120306 (Red Hat 4.6.3-2)\n" +
            "  configuration: \n" +
            "  libavutil      51. 35.100 / 51. 35.100\n" +
            "  libavcodec     53. 61.100 / 53. 61.100\n" +
            "  libavformat    53. 32.100 / 53. 32.100\n" +
            "  libavdevice    53.  4.100 / 53.  4.100\n" +
            "  libavfilter     2. 61.100 /  2. 61.100\n" +
            "  libswscale      2.  1.100 /  2.  1.100\n" +
            "  libswresample   0.  6.100 /  0.  6.100\n" +
            "Input #0, wav, from 'thermo.wav':\n" +
            "  Duration: 03:22:18.13, bitrate: 0 kb/s\n" +
            "    Stream #0:0: Audio: pcm_u8 ([1][0][0][0] / 0x0001), 11025 Hz, 1 channels, u8, 88 kb/s\n" +
            "\n\n" +
            "\n");

    @Before
    public void setUp() {
        MigrateMp3ToWav.MigrationMapper mapper = new MigrateMp3ToWav.MigrationMapper();
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
        mapDriver.withOutput(output, new LongWritable(0));
        mapDriver.runTest();
    }

    @Test
    public void testReducer() {
        List<LongWritable> values = new ArrayList<LongWritable>();
        values.add(new LongWritable(0));
        values.add(new LongWritable(0));
        reduceDriver.withInput(output, values);
        reduceDriver.withOutput(output, new LongWritable(0));
        reduceDriver.runTest();
    }

    @Test
    public void testMapReduce() {
        mapReduceDriver.withInput(new LongWritable(0), inputFilePath);
        mapReduceDriver.addOutput(output, new LongWritable(0));
        mapReduceDriver.runTest();
    }
}
