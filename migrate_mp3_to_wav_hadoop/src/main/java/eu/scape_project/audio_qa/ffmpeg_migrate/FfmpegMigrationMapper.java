package eu.scape_project.audio_qa.ffmpeg_migrate;

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
 * The map function of FfmpegMigrationMapper migrates the mp3 files referenced in input to wav using ffmpeg.
 * The map function returns the path to output directory with the result files.
 *
 * The input is a line number as key (not used) and a Text line, which we assume is the path to an mp3 file.
 * The output is an exit code (not used), and the path to an output directory.
 *
 * eu.scape_project.audio_qa.ffmpeg_migrate
 * User: baj@statsbiblioteket.dk
 * Date: 2014-01-14
 */
public class FfmpegMigrationMapper extends Mapper<LongWritable, Text, LongWritable, Text> {

    private Log log = new Log4JLogger("FfmpegMigrationMapper Log");

    @Override
    protected void map(LongWritable lineNo, Text inputMp3path, Context context) throws IOException, InterruptedException {

        if (inputMp3path.toString().equals("")) return;

        //create a file-specific output dir
        String[] inputSplit = inputMp3path.toString().split("/");
        String inputMp3 = inputSplit.length > 0 ? inputSplit[inputSplit.length - 1] : inputMp3path.toString();
        String[] inputMp3Split = inputMp3.split("\\.");
        String inputMp3Name = inputMp3Split.length > 0 ? inputMp3Split[0] : inputMp3;

        //File outputDir = new File(context.getConfiguration().get("map.outputdir", AudioQASettings.OUTPUT_DIR), inputMp3Name);
        String outputDirPath = context.getConfiguration().get("map.outputdir", AudioQASettings.OUTPUT_DIR) + inputMp3Name;
        //write output directory path to the output key text
        Text output = new Text(outputDirPath);
        log.debug(outputDirPath);
        System.out.println(outputDirPath);

        FileSystem fs = FileSystem.get(context.getConfiguration());
        boolean succesfull = fs.mkdirs(new Path(outputDirPath));//todo permissions (+ absolute path?)
        log.debug("fs.mkdirs() successfull = " + succesfull);
        System.out.println("outputDir.mkdirs() successfull = " + succesfull);

        //migrate with ffmpeg
        String ffmpeglog = outputDirPath + "/" + inputMp3 + "_ffmpeg.log";
        //fs.create(new Path(ffmpeglog));
        String[] ffmpegcommand = new String[5];
        ffmpegcommand[0] = "ffmpeg";
        ffmpegcommand[1] = "-y";
        ffmpegcommand[2] = "-i";
        ffmpegcommand[3] = inputMp3path.toString();
        String outputwav = outputDirPath + "/" + inputMp3 + "_ffmpeg.wav";
        //File outputwav = new File(outputDirPath + "/", inputMp3 + "_ffmpeg.wav");
        //outputwav.setReadable(true, false);
        //outputwav.setWritable(true, false);
        ffmpegcommand[4] = outputwav;
        int exitCode = CLIToolRunner.runCLItool(ffmpegcommand, ffmpeglog, fs);

        context.write(new LongWritable(exitCode), output);
    }

}