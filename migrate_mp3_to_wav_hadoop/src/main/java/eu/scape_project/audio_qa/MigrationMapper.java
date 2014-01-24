package eu.scape_project.audio_qa;

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
 * The map function characterises the mp3 files referenced in input and migrates them to wav.
 * The map function returns the path to output directory with the result files.
 *
 * The input is a line number as key (not used) and a Text line, which we assume is the path to an mp3 file.
 * The output is the path to an output directory, and an exit code (not used).
 *
 */
public class MigrationMapper extends Mapper<LongWritable, Text, LongWritable, Text> {

    private Log log = new Log4JLogger("MigrationMapper Log");

    @Override
    protected void map(LongWritable lineNo, Text inputMp3path, Context context) throws IOException, InterruptedException {

        if (inputMp3path.toString().equals("")) return;

        //create a file-specific output dir
        String[] inputSplit = inputMp3path.toString().split("/");
        String inputMp3 = inputSplit.length > 0 ? inputSplit[inputSplit.length - 1] : inputMp3path.toString();
        String[] inputMp3Split = inputMp3.split("\\.");
        String inputMp3Name = inputMp3Split.length > 0 ? inputMp3Split[0] : inputMp3;

        String outputDirPath = context.getConfiguration().get("map.outputdir", AudioQASettings.OUTPUT_DIR) + inputMp3Name;
        FileSystem fs = FileSystem.get(context.getConfiguration());
        boolean succesfull = fs.mkdirs(new Path(outputDirPath));//todo permissions (+ absolute path?)
        //outputDir.mkdirs();
        //outputDir.setReadable(true, false);
        //outputDir.setWritable(true, false);
        log.debug(outputDirPath);
        //write output directory to the output key text
        Text output = new Text(outputDirPath);

        //copy the input mp3 to output directory for qa???

        //start with ffprobe
        String outputFileString = outputDirPath + "/" + inputMp3 + "_ffprobe.log";
        String [] ffprobeCommand = new String[2];
        ffprobeCommand[0] = "ffprobe";
        ffprobeCommand[1] = inputMp3path.toString();
        int exitCode = CLIToolRunner.runCLItool(ffprobeCommand, outputFileString, fs);
        File outputFile = new File(outputFileString);
        outputFile.setReadable(true, false);
        outputFile.setWritable(true, false);

        //next migrate with ffmpeg
        if (exitCode == 0) {
            String ffmpeglog = outputDirPath + "/" + inputMp3 + "_ffmpeg.log";
            //File logFile = new File(ffmpeglog);
            //logFile.setReadable(true, false);
            //logFile.setWritable(true, false);
            String[] ffmpegcommand = new String[5];
            ffmpegcommand[0] = "ffmpeg";
            ffmpegcommand[1] = "-y";
            ffmpegcommand[2] = "-i";
            ffmpegcommand[3] = inputMp3path.toString();
            String outputwavPath = outputDirPath + "/" + inputMp3 + "_ffmpeg.wav";
            //outputwav.setReadable(true, false);
            //outputwav.setWritable(true, false);
            ffmpegcommand[4] = outputwavPath;
            exitCode = CLIToolRunner.runCLItool(ffmpegcommand, ffmpeglog, fs);
        }

        context.write(new LongWritable(exitCode), output);
    }

    public boolean recursiveDeleteDir(File outputDir) {
        if (outputDir.exists()) {
            if (outputDir.isDirectory()) {
                File[] outputDirFiles = outputDir.listFiles();
                if (outputDirFiles != null) {
                    for (File outputDirFile : outputDirFiles) recursiveDeleteDir(outputDirFile);
                }
            }
            return outputDir.delete();
        }
        return true;
    }

}