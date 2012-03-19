# Setting up Amazon Elastic MapReduce

## Table of Contents

1. [Before you get started](#intro)
2. [Self-hosting the tracking pixel](#pixelsh)
3. [Self-hosting snowplow.js](#jssh)
5. [A note on privacy](#privacy)

<a name="intro"/>
## Before you get started...

Note: this guide assumes you are self-hosting SnowPlow on your own Amazon Web Services account. 

Once you have [integrated snowplow.js into your site](#https://github.com/snowplow/snowplow/blob/master/docs/03_integrating_snowplowjs.md) and [hosted SnowPlow hosting](#https://github.com/snowplow/snowplow/blob/master/docs/04_selfhosting_snowplow.md), you should be happily tracking users across your websites and storing that tracking data as logs in S3. Now you'll want to setup Amazon Elastic MapReduce and Hive to analyse those logs and generate actionable insight.

Amazon has published a very good [getting started](#http://docs.amazonwebservices.com/ElasticMapReduce/latest/GettingStartedGuide/Welcome.html?r=7956). This guide can be used as a standalone guide, or read in connection with Amazon's own guide.

To use Elastic MapReduce you will need to install Amazon's "EMR Command Line Interface". This will, in particular, let you run "Hive interactive sessions" in which you can try different queries and develop differnet analyses, using the results of each analysis to inform the next query.

## Installing Ruby

"EMR Command Line Interface" is built in Ruby, so unless you have already have Ruby installed, you'll need to install it. Full instructions on downloading and setting up Ruby can be found [here](#http://www.ruby-lang.org/en/downloads/). There are many ways to install Ruby - if you don't have a strong preference for one of them, we recommend Mac OS X and Linux users use RVM, whilst Windows users use Ruby Installer.

### Installing Ruby on Windows

* Go to [RubyInstaller](#http://rubyinstall.org). Click "Download"

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

* Go to [http://aws.amazon.com/developertools/2264](#http://aws.amazon.com/developertools/2264). Login if prompted and click download.

![Download CLI](#snowplow/snowplow/raw/master/docs/images/emr-guide/install-cli-2.PNG)

* Move the ZIP file to the folder you just created, and then unzip it

![Unzip CLI](/snowplow/snowplow/raw/master/docs/images/emr-guide/install-cli-3.PNG)

### Create a configuration file, with your login credentials

* To use the command line tools successfully, you will need the tools to talk directly to your Amazon account without any difficulty. That means ensuring that the correct security / login details are available to the command-line tools

* Start by fetching your AWS Access Key ID and AWS Secret Access Key. To get both of these, log in to [http://aws.amazon.com](#http://aws.amazon.com)

![Download CLI](/snowplow/snowplow/raw/master/docs/images/emr-guide/install-cli-4.PNG)

* Click on *My Account* (by clicking on your name on the top right of the screen) and select *Security Credentials*. Scroll down to *Access Credentials*: you will be able to take a copy of your *Access Key ID*. To reveal the associated *Secret Access Key*, click on the _Show_ link. Copy and paste both of these to a text-editor: in a bit you will use these to create a config file.

![Download CLI](/snowplow/snowplow/raw/master/docs/images/emr-guide/install-cli-5.png)

* Now you have your *access key* and *secret access key*, you need to create an *Amazon EC2 Key Pair*. Elastic MapReduce is built on top of EC2, and Key Pairs are a key part of Amazon's security apparatus. Click on the *Key Pairs* tab (2 along from *Access Keys* in the same screen) and click *Access your Amazon EC2 Key Pairs using the AWS Management Console*. (Don't be distracted bythe CloudFront Key Pairs section above - that is not relevant here...)

![Navigate to key pair](/snowplow/snowplow/raw/master/docs/images/emr-guide/install-cli-6.PNG)


* In the EC2 window, click on *Key Pairs* on the left hand navigation. (In the _Network & Security_ section.) 

![Create key pair](/snowplow/snowplow/raw/master/docs/images/emr-guide/install-cli-7.PNG)

* Click on the *Create Key Pair* button

![Create key pair](/snowplow/snowplow/raw/master/docs/images/emr-guide/install-cli-8.PNG)

* When prompted, give the key pair an appropriate name of your choice. Note it down in your text-editor

![Name new key pair](/snowplow/snowplow/raw/master/docs/images/emr-guide/install-cli-9.PNG)

* The `.PEM` file should automatically download. Move it to a safe location (e.g. in a subdirectory of `elastic-mapreduce-cli`) and note the location, again in your text editor, along with the name of the key pair. (This should be the same as the name of the `.PEM` file)

* Now go to your text-editor. Create a file `credentials.json` in the `elastic-mapreduce-cli` folder

* Add the following (JSON) code to it:
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



