package eu.scape_project.audio_qa;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.File;
import java.io.IOException;

/**
 * eu.scape_project.audio_qa
 * User: baj@statsbiblioteket.dk
 * Date: 8/30/13
 *
 * The map function performs QA on the wav file in the input directory referenced in input.
 * The map function works in the input directory and returns only "QA passed true/false".
 *
 * The input is a line number as key (not used) and a Text line, which we assume is the path to an input directory.
 * The output is a boolean as Text "QA passed true/false", and an exit code (not used).
 *
 */
public class QAMapper extends Mapper<LongWritable, Text, Text, LongWritable> {

    @Override
    protected void map(LongWritable lineNo, Text inputDir, Context context) throws IOException, InterruptedException {

        if (inputDir.toString().equals("")) return;

        //TODO convert original mp3 to wav using mpg321 for comparison
        //but where is it referenced?
        //maybe Text Text input is better (input mp3, working directory)?

    }

}
