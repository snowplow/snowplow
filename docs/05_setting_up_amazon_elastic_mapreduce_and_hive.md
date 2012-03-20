# Setting up Amazon Elastic MapReduce

*Please note! This document is a work in progress, and is actively being editted. We recommend not using it at the moment. (It will be completed in the next couple of days...)*

## Table of Contents

1. [Before you get started](#intro)
2. [Self-hosting the tracking pixel](#pixelsh)
3. [Self-hosting snowplow.js](#jssh)
5. [A note on privacy](#privacy)

<a name="intro"/>
## Before you get started...

Note: this guide assumes you are self-hosting SnowPlow on your own Amazon Web Services account. 

Once you have [integrated snowplow.js into your site](https://github.com/snowplow/snowplow/blob/master/docs/03_integrating_snowplowjs.md) and [hosted SnowPlow hosting](https://github.com/snowplow/snowplow/blob/master/docs/04_selfhosting_snowplow.md), you should be happily tracking users across your websites and storing that tracking data as logs in S3. Now you'll want to setup Amazon Elastic MapReduce and Hive to analyse those logs and generate actionable insight.

Amazon has published a very good [getting started](http://docs.amazonwebservices.com/ElasticMapReduce/latest/GettingStartedGuide/Welcome.html?r=7956). This guide can be used as a standalone guide, or read in connection with Amazon's own guide.

To use Elastic MapReduce you will need to install Amazon's "EMR Command Line Interface". This will, in particular, let you run "Hive interactive sessions" in which you can try different queries and develop differnet analyses, using the results of each analysis to inform the next query.

## Installing Ruby

"EMR Command Line Interface" is built in Ruby, so unless you have already have Ruby installed, you'll need to install it. Full instructions on downloading and setting up Ruby can be found [here](#http://www.ruby-lang.org/en/downloads/). There are many ways to install Ruby - if you don't have a strong preference for one of them, we recommend Mac OS X and Linux users use RVM, whilst Windows users use Ruby Installer.

### Installing Ruby on Windows

* Go to [RubyInstaller](http://rubyinstaller.org/). Click "Download"

![Downloading Ruby](/snowplow/snowplow/raw/master/docs/images/emr-guide/ruby-1.PNG)

* Select the most recent RubyInstaller. The download should begin immediately

![Select the latest Ruby verison](/snowplow/snowplow/raw/master/docs/images/emr-guide/ruby-2.PNG)

* Launch the executable file you just downloaded, by double clicking on it. Accept the license terms. When prompted, remember to "Add Ruby executables to your PATH". We also recommend checking the box "Associate .rb and .rbw files with this Ruby installation."

![Select the relevant options](/snowplow/snowplow/raw/master/docs/images/emr-guide/ruby-5.PNG)

* The installation should proceed automatically. When completed, click "finish".

* Verify the installation: in either the command-prompt or Windows PowerShell, enter "ruby -v". The Ruby version should display as below:

![Verify installation was successful](/snowplow/snowplow/raw/master/docs/images/emr-guide/ruby-6.PNG)

### Installing Ruby on Mac OS X / Linux 

To do

### Installing the EMR Command-Line tools

* Navigate to your Ruby folder, and in it, create a new folder for the command line tools:

![Create a directory for the command-line tools](/snowplow/snowplow/raw/master/docs/images/emr-guide/install-cli-1.PNG)

* Go to [http://aws.amazon.com/developertools/2264](http://aws.amazon.com/developertools/2264). Login if prompted and click download.

![Download CLI](/snowplow/snowplow/raw/master/docs/images/emr-guide/install-cli-2.PNG)

* Go to the folder you just installed Ruby in, and in it, create a new folder where you'll save the Elastic Map Reduce tools. Give this folder an appropriate name e.g. `elastic-mapreduce-cli`. Unzip the download into the new folder, by double clicking on the Zip file (to access the contents), selecting the contents, copying it and then pasting it into the new folder.

![Unzip CLI](/snowplow/snowplow/raw/master/docs/images/emr-guide/install-cli-3.PNG)

### Create a configuration file, with your login credentials

* To use the command line tools successfully, you will need the tools to talk directly to your Amazon account without any difficulty. That means ensuring that the correct security / login details are available to the command-line tools

* Start by fetching your AWS Access Key ID and AWS Secret Access Key. To get both of these, log in to [http://aws.amazon.com](#http://aws.amazon.com)

![Download CLI](/snowplow/snowplow/raw/master/docs/images/emr-guide/install-cli-4.PNG)

* Click on *My Account* (by clicking on your name on the top right of the screen) and select *Security Credentials*. Scroll down to *Access Credentials*: you will be able to take a copy of your *Access Key ID*. To reveal the associated *Secret Access Key*, click on the _Show_ link. Copy and paste both of these to a text-editor: in a bit you will use these to create a config file.

![Download CLI](/snowplow/snowplow/raw/master/docs/images/emr-guide/install-cli-5.png)

* Now you have your *access key* and *secret access key*, you need to create an *Amazon EC2 Key Pair*. Elastic MapReduce is built on top of EC2, and Key Pairs are a key part of Amazon's security apparatus. Click on the *Key Pairs* tab (2 along from *Access Keys* in the same screen) and click *Access your Amazon EC2 Key Pairs using the AWS Management Console*. (Don't be distracted by the CloudFront Key Pairs section above - that is not relevant here...)

![Navigate to key pair](/snowplow/snowplow/raw/master/docs/images/emr-guide/install-cli-6.PNG)

* In the EC2 window, check what region has been set in the top left, and if necessary, change the region to the one in which you plan to do your analysis. (We use EU West (Ireland) for most of our analysis because we're based in the UK, but Amazon still often defaults to one of the US data center locations...)

* Click on *Key Pairs* on the left hand navigation. (In the _Network & Security_ section.) 

![Create key pair](/snowplow/snowplow/raw/master/docs/images/emr-guide/install-cli-7.PNG)

* Click on the *Create Key Pair* button

![Create key pair](/snowplow/snowplow/raw/master/docs/images/emr-guide/install-cli-8.PNG)

* When prompted, give the key pair an appropriate name of your choice. Note it down in your text-editor

![Name new key pair](/snowplow/snowplow/raw/master/docs/images/emr-guide/install-cli-0.PNG)

* The `.PEM` file should automatically download. Move it to a safe location (e.g. in a subdirectory of `elastic-mapreduce-cli`) and note the location, again in your text editor, along with the name of the key pair. (This should be the same as the name of the `.PEM` file)

* Now go to your text-editor. Create a file `credentials.json` in the `elastic-mapreduce-cli` folder

* Add the following (JSON) code to it, substituting the relevant values, noted above, for `access_id`, `private_key`, `keypair`, `key-pair-file`:

	```javascript	
	{
		"access_id": "[Your AWS Access Key ID. (See above)]",
		"private_key": "[Your AWS Secret Access Key. (See above)]",
		"keypair": "[Your key pair name. (See above)]",
		"key-pair-file": "[The path and name of your PEM file. (See above)]",
		"log_uri": "[A path to the bucket on S3 where your SnowPLow logs are kept. We will identify this in the next section.]",
		"region": "[The Region of yoru job flow, either us-east-1, us-west-2, us-east-1, eu-west-1", eu-west-1, ap-northeast-1, ap-southeast-1, or sa-east-1. We will identify this in the next section]"
	}
	
	```

* The `log-uri` parameter in the `credentials.json` file needs to point at the Amazon S3 bucket that you will use to store the outputs of your analysis. (And any logging of Hadoop sessions, if you desire.) It makes sense to create a new bucket to store these results. To do so, click on the `S3` tab at the top of the _AWS Management Console_, and in the left hand menu under _Buckets_ click the *Create Bucket* button:

![Name new S3 bucket to house analysis](/snowplow/snowplow/raw/master/docs/images/emr-guide/install-cli-11.PNG)

* Name the bucket. (You'll need top pick a name that is unique across Amazon S3, to `snowplow-analysis` will not be an option, unfortunately 

* Select which Region you want the bucket located in. Because we're based in the UK, we've picked Ireland: pick the data center that makes the most sense for you. (Most likely the same location as the S3 buckets with the raw SnowPlow data you intend to analyse.) Click the *create* button.

* Now update `credentials.json` with the `log-uri`. This will be `s3n://` + the bucket name + `/`. For us, then 

	`"log_uri" = "s3n://snowplow-analysis/"`

* You also need to set the `region` field in `credentials.json`. Pick the appropriate region - this should match the Region you selected when you created the bucket. So for us that is `eu-west-1`. (we selected _Ireland_)

<table>
	<tr><td>Region name</td><td>Region code</td></tr>
	<tr><td>US Standard</td><td>us-east-1</td></tr>
	<tr><td>Oregon</td><td>us-west-2</tr>
	<tr><td>Northern California</td><td>us-west-1</td></tr>
	<tr><td>Ireland</td><td>eu-west-1</td></tr>
	<tr><td>Japan</td><td>ap-northeast-1</td></tr>
	<tr><td>Singapore</td><td>ap-southeast-1</td></tr>
	<tr><td>Sao Paulo</td><td>sa-east-1</td></tr>
</table>

* You need to set permissions on the S3 bucket, so that the command line tools can write results output to it. To do so, go back to the [S3 console](https://console.aws.amazon.com/s3/), right click on the bucket you created and click on *properties*

![Name new S3 bucket to house analysis](/snowplow/snowplow/raw/master/docs/images/emr-guide/install-cli-12.PNG)

* A _Properties_ pane will appear at the bottom of the screen. On the _Permissions_ tabl, click *Add more permissions*

![Name new S3 bucket to house analysis](/snowplow/snowplow/raw/master/docs/images/emr-guide/install-cli-13.PNG)

* Select *Authenticated Users* from the *Grantee* dropdown, then select *List* just next to it and click *Save*

![Name new S3 bucket to house analysis](/snowplow/snowplow/raw/master/docs/images/emr-guide/install-cli-14.PNG)

#### SSH Setup: for Mac and Linux

* Navigate to your `.PEM` file in the command line tool and set the permissions on the file as below:

![Fix permissions on .PEM file](/snowplow/snowplow/raw/master/docs/images/emr-guide/mac-ssh-1.tiff)


#### SSH Setup: for Windows

* Download PuTTYgen.exe from [here](http://www.chiark.greenend.org.uk/~sgtatham/putty/download.html)

![Name new S3 bucket to house analysis](/snowplow/snowplow/raw/master/docs/images/emr-guide/install-cli-15a.PNG)

* Double click on the download to launch PuTTYgen. Click on the *Load* button and select the `.PEM` file you downloaded from Amazon earlier, when you created the EC2 key-pair

![Name new S3 bucket to house analysis](/snowplow/snowplow/raw/master/docs/images/emr-guide/install-cli-15a.PNG)

![Name new S3 bucket to house analysis](/snowplow/snowplow/raw/master/docs/images/emr-guide/install-cli-16.PNG)

* Enter a passphrase in the dialogue box, confirm the passphrase, and click the *Save private key* button. Save the file down as a `.ppk` file: this will be what you use to establish a secure SSL connection.

* Exit the PUTTYgen application

* Now download *PUTTY* and *Pageant* from [the same webpage you downloaded PUTTYgen](http://www.chiark.greenend.org.uk/~sgtatham/putty/download.html). You will need these to establish the SSH connection and run Hive

## Running a job

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


