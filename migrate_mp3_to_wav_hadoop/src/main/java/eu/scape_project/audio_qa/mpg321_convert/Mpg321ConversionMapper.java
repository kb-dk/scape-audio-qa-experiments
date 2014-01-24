package eu.scape_project.audio_qa.mpg321_convert;

import eu.scape_project.audio_qa.AudioQASettings;
import eu.scape_project.audio_qa.CLIToolRunner;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.impl.Log4JLogger;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.File;
import java.io.IOException;

/**
 * The map function of Mpg321ConversionMapper converts the mp3 files referenced in input to wav using mpg321.
 * The map function returns the path to output directory with the result files.
 *
 * The input is a line number as key (not used) and a Text line, which we assume is the path to an mp3 file.
 * The output is an exit code (not used), and the path to an output directory.
 *
 * eu.scape_project.audio_qa.mpg321_convert
 * User: baj@statsbiblioteket.dk
 * Date: 2014-01-15
 */
public class Mpg321ConversionMapper extends Mapper<LongWritable, Text, LongWritable, Text> {

    private Log log = new Log4JLogger("Mpg321ConversionMapper Log");

    @Override
    protected void map(LongWritable lineNo, Text inputMp3path, Context context) throws IOException, InterruptedException {

        if (inputMp3path.toString().equals("")) return;

        //create a file-specific output dir
        String[] inputSplit = inputMp3path.toString().split("/");
        String inputMp3 = inputSplit.length > 0 ? inputSplit[inputSplit.length - 1] : inputMp3path.toString();
        String[] inputMp3Split = inputMp3.split("\\.");
        String inputMp3Name = inputMp3Split.length > 0 ? inputMp3Split[0] : inputMp3;

        String outputDirPath = context.getConfiguration().get("map.outputdir", AudioQASettings.OUTPUT_DIR) + inputMp3Name;
        //outputDir.mkdirs();
        //outputDir.setReadable(true, false);
        //outputDir.setWritable(true, false);
        FileSystem fs = FileSystem.get(context.getConfiguration());
        boolean succesfull = fs.mkdirs(new Path(outputDirPath));//todo permissions (+ absolute path?)

        log.debug(outputDirPath);
        //write output directory to the output key text
        Text output = new Text(outputDirPath);

        //convert with mpg321
        String mpg321log = outputDirPath + AudioQASettings.SLASH + inputMp3 + AudioQASettings.UNDERSCORE + AudioQASettings.MPG321 + AudioQASettings.DOTLOG;
        //File logFile = new File(mpg321log);
        //logFile.setReadable(true, false);
        //logFile.setWritable(true, false);
        String[] mpg321command = new String[4];
        mpg321command[0] = AudioQASettings.MPG321;
        mpg321command[1] = "-w";
        String outputwavPath = outputDirPath + AudioQASettings.SLASH + inputMp3 + AudioQASettings.UNDERSCORE + AudioQASettings.MPG321 + AudioQASettings.DOTWAV;
        //outputwav.setReadable(true, false);
        //outputwav.setWritable(true, false);
        mpg321command[2] = outputwavPath;
        mpg321command[3] = inputMp3path.toString();
        int exitCode = CLIToolRunner.runCLItool(mpg321command, mpg321log, fs);

        context.write(new LongWritable(exitCode), output);
    }

}