scape-audio-qa
==============

The Scape Audio QA repo includes tools and workflows used in large scale audio qa.

Use mvn package to create migrate_mp3_to_wav_workflow-0.1-SNAPSHOT-bundle.tar.gz

Move the package to the desired destination and unpack using

tar -zxvf migrate_mp3_to_wav_workflow-0.1-SNAPSHOT-bundle.tar.gz

Change directory to migrate_mp3_to_wav_workflow-0.1-SNAPSHOT

cd migrate_mp3_to_wav_workflow-0.1-SNAPSHOT

Run the full workflow on the test data with this command

./bin/migrateMP3ToWAV.sh $PWD/samples/filelist.txt $PWD

See workflows/README for dependencies and further instructions.