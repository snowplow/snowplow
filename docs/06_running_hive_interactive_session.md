# Running a Hive interactive session

Most of the analyses we do are in Hive interactive sessions: because it is in these types of sessions that we can actively query data, explore results and develop more sophisticated analyses.

New sessions can either be initiated at the command-line, or via aws.amazon.com console. 

To initiative a new session on Mac / Linux, navigate to the `elastic-mapreduce-cli` folder (where you saved the command-line tools) and enter

	$ ./elastic-mapreduce --create --alive --name "Hive Job Flow" --hive-interactive

You should see something like:

![Launch a Hive session from the command-line](/snowplow/snowplow/raw/master/docs/images/emr-guide/run-hive-interactive-session-1.tiff)

TODO: Add instructions to launch a session from the PC command-line

Log into the [Amazon Web Console](https://console.aws.amazon.com/console/home) and click on [Elastic MapReduce] in the top menu bar. You should see the job you created listed. (In the screenshot below you'll see that we've initiated 2 Hive sessions.)

![Launch a Hive session from the command-line](/snowplow/snowplow/raw/master/docs/images/emr-guide/run-hive-interactive-session-2.tiff)

### Establishing the SSH connection: Mac / Linux users

Return to the command-line, establish an SSH connection by entering the following

	$ ./elastic-mapreduce --ssh --jobflow [JobFlowID]

Substituting the JobFlowID generated when you created the session. You should see:

![Launch a Hive session from the command-line](/snowplow/snowplow/raw/master/docs/images/emr-guide/run-hive-interactive-session-3.tiff)

Now you can launch Hive by typing `Hive` at the command line:

![Launch a Hive session from the command-line](/snowplow/snowplow/raw/master/docs/images/emr-guide/run-hive-interactive-session-4.tiff)



