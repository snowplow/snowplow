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

1. Go to [RubyInstaller](#http://rubyinstall.org). Click "Download"

![Downloading Ruby](/snowplow/snowplow/raw/master/docs/images/emr-guide/ruby-1.PNG)

2. Select the most recent RubyInstaller. The download should begin immediately

![Select the latest Ruby verison](/snowplow/snowplow/raw/master/docs/emr-guide/images/ruby-2.PNG)

3. Launch the executable file you just downloaded, by double clicking on it. Accept the license terms. When prompted, remember to "Add Ruby executables to your PATH". We also recommend checking the box "Associate .rb and .rbw files with this Ruby installation."

![Select the relevant options](/snowplow/snowplow/raw/master/docs/images/emr-guide/ruby-5.PNG)

4. The installation should proceed automatically. When completed, click "finish".

5. Verify the installation: in either the command-prompt or Windows PowerShell, enter "ruby -v". The Ruby version should display as below:

![Verify installation was successful](/snowplow/snowplow/raw/master/docs/images/emr-guide/ruby-6.PNG)

### Installing Ruby on Mac OS X / Linux 

To do