# Copyright (c) 2012-2014 Snowplow Analytics Ltd. All rights reserved.
#
# This program is licensed to you under the Apache License Version 2.0,
# and you may not use this file except in compliance with the Apache License Version 2.0.
# You may obtain a copy of the Apache License Version 2.0 at http://www.apache.org/licenses/LICENSE-2.0.
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the Apache License Version 2.0 is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the Apache License Version 2.0 for the specific language governing permissions and limitations there under.

# Author::    Alex Dean (mailto:support@snowplowanalytics.com)
# Copyright:: Copyright (c) 2014 Snowplow Analytics Ltd
# License::   Apache License Version 2.0

require 'spec_helper'

EmrJob = Snowplow::EmrEtlRunner::EmrJob

describe EmrJob do

  it 'gets the correct S3 endpoint for us-east-1' do
    EmrJob.get_s3_endpoint('us-east-1').should eql 's3.amazonaws.com'
  end

  it 'gets the correct S3 endpoint for other regions' do
    EmrJob.get_s3_endpoint('eu-west-1').should eql 's3-eu-west-1.amazonaws.com'
  end

  it 'returns the Hadoop assets' do
    EmrJob.get_assets("s3://hadoop-assets/", "1.0.0").should == {
      :maxmind  => "s3://hadoop-assets/third-party/maxmind/GeoLiteCity.dat",
      :s3distcp => "/home/hadoop/lib/emr-s3distcp-1.0.jar",
      :hadoop   => "s3://hadoop-assets/3-enrich/hadoop-etl/snowplow-hadoop-etl-1.0.0.jar"
    }
  end

end
