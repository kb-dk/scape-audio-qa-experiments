# Migrate mp3 files to wav files and perform audio qa using Hadoop

The migrate-mp3-to-wav-hadoop module contains a number of Hadoop Jobs. These are

* FfmpegMigration
* Mpg321Conversion
* WaveformCompare

which all perform exactly one task. And

* MigrateMp3ToWav, which uses the Hadoop Chainmapper to include
  * MigrationMepper, which does both the ffmpeg migration and the ffprobe characterisation of the mp3 file and
  * QAMapper, which does the ffprobe characterisation of the migrated wav file, the property comparison,
  the mpg321 conversion, and the waveform-compare.

## Ffmpeg Migration

The input to this job is a list of mp3 file paths to mp3 files on NFS. The text file with
the list of mp3 file paths should be available on HDFS.

TBD

## Mpg321Conversion

The input to this job is a list of mp3 file paths to mp3 files on NFS. The text file with
the list of mp3 file paths should be available on HDFS.

TBD

## WaveformCompare

The input to this job is a list of pairs of file paths to wav files on NFS. The text file with
the list of pairs of file paths should be available on HDFS.

TBD

## MigrateMp3ToWav

The input to this job is a list of mp3 file paths to mp3 files already available on HDFS. The text file with
the list of mp3 file paths should also be available on HDFS.

To run the job first run 'mvn package' to get 'migrate_mp3_to_wav_hadoop-0.1-SNAPSHOT-jar-with-dependencies.jar'

Then run
'hadoop jar migrate_mp3_to_wav_hadoop-0.1-SNAPSHOT-jar-with-dependencies.jar eu.scape_project.audio_qa.MigrateMp3ToWav
-Dmapred.max.split.size=1024 <input/mp3list.txt> <output/map-reduce-result-output-dir> <output/mp3-to-wav-output-dir>'

where <input/mp3list.txt> is the path on hdfs to the text file with the list of mp3 file paths (on hdfs),
<output/map-reduce-result-output-dir> is the output directory for the map-reduce result (on hdfs)
and <output/mp3-to-wav-output-dir> is the output directory for the migration and qa results (on hdfs).

The -Dmapred.max.split.size=1024 parameter is the easy way of splitting the input text files into smaller pieces.
The actual value can be adjusted. This will be changed to use number of lines (i.e. input files) instead.

The reason for two output directories is that to have the actual migrated files on NFS, and the qa results on HDFS,
such that we can reduce these / do statistics on them using MR. The reason for three output directories is mostly
historic.
