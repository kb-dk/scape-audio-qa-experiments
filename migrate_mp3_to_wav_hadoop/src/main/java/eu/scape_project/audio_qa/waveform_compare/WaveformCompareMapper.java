package eu.scape_project.audio_qa.waveform_compare;

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
import java.util.Arrays;

/**
 * The map function of WaveformCompareMapper uses xcorrSound waveform-compare to compare content of the wav files
 * referenced in input. The map function returns the output of the waveform-compare tool.
 *
 * The input is a line number as key (not used) and a Text line, which we assume is two tab-separated path to two
 * wav files to be compared.
 * The output is an exit code, and todo the output of the waveform-compare tool (now path to log file).
 *
 * eu.scape_project.audio_qa.mpg321_convert
 * User: baj@statsbiblioteket.dk
 * Date: 2014-01-20
 */
public class WaveformCompareMapper extends Mapper<LongWritable, Text, LongWritable, Text> {

    private Log log = new Log4JLogger("WaveformCompareMapper Log");

    @Override
    protected void map(LongWritable lineNo, Text wavfilepathpair, Context context) throws IOException, InterruptedException {

        if (wavfilepathpair.toString().equals("")) {
            log.info("empty input line");
            System.out.println("empty input line");
            return;
        }

        //get input wavs
        String[] inputSplit = wavfilepathpair.toString().split("[ \\t\\n\\x0B\\f\\r]");//splits on whitespace
        if (inputSplit.length<2) {
            log.info("input line not formatted correctly:\n" + wavfilepathpair.toString());
            return;
        }
        String inputWav1 = inputSplit[0];
        String inputWav2 = inputSplit[inputSplit.length-1];

        //create a hadoop-job-specific output dir
        String outputDirPath;
        if (context.getJobID()==null) {
            outputDirPath = context.getConfiguration().get("map.outputdir", AudioQASettings.MAPPER_OUTPUT_DIR) +
                    context.getConfiguration().get("job.jobID", AudioQASettings.DEFAULT_JOBID);
        } else {
            outputDirPath = context.getConfiguration().get("map.outputdir", AudioQASettings.MAPPER_OUTPUT_DIR) +
                    context.getJobID().toString();
        }
        //create output log file
        String[] inputWav1Split = inputWav1.split(AudioQASettings.SLASH);
        String inputWav1Name = inputWav1Split.length > 0 ? inputWav1Split[inputWav1Split.length - 1] : inputWav1;
        String[] inputWav1NameSplit = inputWav1Name.split("\\.");
        String logFileName = inputWav1NameSplit.length > 0 ? inputWav1NameSplit[0] : inputWav1Name;
        String logFilePath = outputDirPath + AudioQASettings.SLASH + logFileName +
                AudioQASettings.UNDERSCORE + "compare" + AudioQASettings.DOTLOG;
        //logFile.setReadable(true, false);
        //logFile.setWritable(true, false);
        Text output = new Text(logFilePath);

        FileSystem fs = FileSystem.get(context.getConfiguration());
        boolean succesfull = fs.mkdirs(new Path(outputDirPath));//todo permissions (+ absolute path?)
        //outputDir.mkdirs();
        //outputDir.setReadable(true, false);
        //outputDir.setWritable(true, false);
        log.debug(outputDirPath);

        //compare wav file content with waveform-compare
        String[] wavCompareCommand = new String[3];
        wavCompareCommand[0] = "waveform-compare";
        wavCompareCommand[1] = inputWav1;
        wavCompareCommand[2] = inputWav2;
        int exitCode = CLIToolRunner.runCLItool(wavCompareCommand, logFilePath, fs);

        context.write(new LongWritable(exitCode), output);
    }

}
