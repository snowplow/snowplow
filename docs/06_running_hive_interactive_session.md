# Running a Hive interactive session

Most of the analyses we perform in SnowPlow use Hive. We tend to use Hive Interactive Sessions to develop queries and analyses. If we want to repeat these analyses over time, we turn these into formal scripts that can be regularly run in an automated way.

This part of the guide walks through the process of launching and running a Hive Interactive session. The steps involved are:

1. [Starting a job](#startingajob) i.e. firing up a set of instances to run Hive / Hadoop
2. [SSHing in to the instances and launching Hive](#sshin)
3. [Running Hive queries, including using the SnowPlow serde](#runninghivequeries)
4. [Terminating the session](#terminatingthesession.)

Most of the analyses we perform are in Hive interactive sessions: because it is in these types of sessions that we can actively query data, explore results and develop more sophisticated analyses.

New sessions can either be initiated at the command-line, or via aws.amazon.com console. 

<a name="startingajob"/>
## Starting a job

There are 2 ways to start a job / fire up instances to run Hive: 

1. [Using the Ruby Client](#usingtherubyclient)
2. [From the Amazon Web UI](#fromtheamazonwebui)

<a name="usingtherubyclient"/>
### Starting a job using the Ruby Client

#### Starting a job using the Ruby Client on Mac / Linux

To initiative a new session on Mac / Linux, navigate to the `elastic-mapreduce-cli` folder (where you saved the command-line tools) and enter

	$ ./elastic-mapreduce --create --alive --name "Hive Job Flow" --hive-interactive

You should see something like:

![Launch a Hive session from the command-line](/snowplow/snowplow/raw/master/docs/images/emr-guide/run-hive-interactive-session-1.jpg)

#### Starting a job using the Ruby Client on PC

TO WRITE: Add instructions to launch a session from the PC command-line, incl. setting up Putty and Pageant to SSH successfully

### Starting a job using the web UI

Amazon provides a web UI that makes initiating Hive jobs very easy.

### Checking that the job has been setup using the Amazon web UI

Note: this will work however you initiated the job. (Whether you used the Ruby Client or the Web UI.)

Log into the [Amazon Web Console](https://console.aws.amazon.com/console/home) and click on [Elastic MapReduce] in the top menu bar. You should see the job you created listed. (In the screenshot below you'll see that we've initiated 2 Hive sessions.)

![Launch a Hive session from the command-line](/snowplow/snowplow/raw/master/docs/images/emr-guide/run-hive-interactive-session-2.jpg)

<a name="sshin"/>
## Establishing an SSH connection

### Establishing the SSH connection: Mac / Linux users

Return to the command-line, establish an SSH connection by entering the following

	$ ./elastic-mapreduce --ssh --jobflow [JobFlowID]

Substituting the JobFlowID generated when you created the session. You should see:

![Launch a Hive session from the command-line](/snowplow/snowplow/raw/master/docs/images/emr-guide/run-hive-interactive-session-3.jpg)

Now you can launch Hive by typing `Hive` at the command line:

![Launch a Hive session from the command-line](/snowplow/snowplow/raw/master/docs/images/emr-guide/run-hive-interactive-session-4.jpg)

### Establishing the SSH connection: PC users

TO WRITE

<a name="runninghivequeries"/>
## Running Hive queries and using the SnowPlow serde

TO WRITE

<a name="terminatingthesession"/>
## Terminating the session

Interactive sessions have to be terminated manually. (Or else you risk running up high Amazon fees...) Sessions can either be terminated at the command-line, using the Ruby Client, or via the web UI.

### Terminating the session using the web UI

TO WRITE

### Terminating the session at the command-line using the Ruby Client

TO WRITE
