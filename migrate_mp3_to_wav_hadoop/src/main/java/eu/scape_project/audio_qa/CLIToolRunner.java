package eu.scape_project.audio_qa;

import java.io.*;

/**
 *
 * eu.scape_project
 * User: baj@statsbiblioteket.dk
 * Date: 8/7/13
 * Time: 11:57 AM
 */
public class CLIToolRunner {
    static int runCLItool(String[] commandline, String logFile) throws IOException {
        ProcessBuilder pb = new ProcessBuilder(commandline);
        //start the executable
        Process proc = pb.start();
        BufferedReader stdout = new BufferedReader(new InputStreamReader(proc.getInputStream()));
        BufferedReader stderr = new BufferedReader(new InputStreamReader(proc.getErrorStream()));
        try {
            //wait for process to end before continuing
            proc.waitFor();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        int exitCode = proc.exitValue();
        String stdoutString = "";
        while (stdout.ready()) {
            stdoutString += stdout.readLine() + "\n";
        }
        String stderrString = "";
        while (stderr.ready()) {
            stderrString += stderr.readLine() + "\n";
        }

        BufferedWriter logFileWriter = new BufferedWriter(new FileWriter(logFile, true));
        //write log of stdout and stderr to the log file
        logFileWriter.write(stdoutString + stderrString);
        logFileWriter.newLine();
        logFileWriter.close();
        return exitCode;
    }
}
