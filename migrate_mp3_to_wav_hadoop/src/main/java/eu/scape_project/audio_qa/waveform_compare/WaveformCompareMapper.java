package eu.scape_project.audio_qa.waveform_compare;

import eu.scape_project.audio_qa.AudioQASettings;
import eu.scape_project.audio_qa.CLIToolRunner;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.impl.Log4JLogger;
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
    protected void map(LongWritable lineNo, Text inputMp3path, Context context) throws IOException, InterruptedException {

        if (inputMp3path.toString().equals("")) {
            log.info("empty input line");
            System.out.println("empty input line");
            return;
        }

        //get input wavs
        String[] inputSplit = inputMp3path.toString().split("[ \\t\\n\\x0B\\f\\r]");//splits on whitespace
        if (inputSplit.length<2) {
            log.info("input line not formatted correctly:\n" + inputMp3path.toString());
            return;
        }
        String inputWav1 = inputSplit[0];
        String inputWav2 = inputSplit[inputSplit.length-1];

        //create output file
        int i = 1;
        while (inputWav1.startsWith(inputWav2.substring(0,i))) i++;
        String outputFileName = inputWav1;
        if (i>1) outputFileName = inputWav1.substring(0, i-1);
        String[] outputFileNameTMpSplit = outputFileName.split(AudioQASettings.SLASH);
        outputFileName = outputFileNameTMpSplit[outputFileNameTMpSplit.length-1];
        outputFileName = outputFileName + AudioQASettings.UNDERSCORE + "compare" + AudioQASettings.DOTLOG;
        //create a hadoop-job-specific output dir
        File outputDir;
        if (context.getJobID()==null) {
            outputDir = new File(context.getConfiguration().get("map.outputdir", AudioQASettings.OUTPUT_DIR),
                    context.getConfiguration().get("job.jobID", AudioQASettings.DEFAULT_JOBID));
        } else {
            outputDir = new File(context.getConfiguration().get("map.outputdir", AudioQASettings.OUTPUT_DIR),
                    context.getJobID().toString());
        }
        File logFile = new File(outputDir, outputFileName);
        logFile.setReadable(true, false);
        logFile.setWritable(true, false);
        Text output = new Text(outputFileName);


        outputDir.mkdirs();
        outputDir.setReadable(true, false);
        outputDir.setWritable(true, false);
        log.debug(outputDir);

        //compare wav file content with waveform-compare
        String[] wavCompareCommand = new String[3];
        wavCompareCommand[0] = "waveform-compare";
        wavCompareCommand[1] = inputWav1;
        wavCompareCommand[2] = inputWav2;
        int exitCode = CLIToolRunner.runCLItool(wavCompareCommand, logFile.toString());

        context.write(new LongWritable(exitCode), output);
    }

}
