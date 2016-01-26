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
    EmrJob.get_assets("s3://hadoop-assets/", "1.0.0", "0.3.0", "0.1.0").should == {
      :enrich   => "s3://hadoop-assets/3-enrich/scala-hadoop-enrich/snowplow-hadoop-enrich-1.0.0.jar",
      :shred    => "s3://hadoop-assets/3-enrich/scala-hadoop-shred/snowplow-hadoop-shred-0.3.0.jar",
      :elasticsearch => "s3://hadoop-assets/4-storage/hadoop-elasticsearch-sink/hadoop-elasticsearch-sink-0.1.0.jar"
    }
  end

  it 'returns the Hadoop assets for old versions of Hadoop Shred' do
    EmrJob.get_assets("s3://hadoop-assets/", "0.14.2", "0.3.0", "0.1.0").should == {
      :enrich   => "s3://hadoop-assets/3-enrich/hadoop-etl/snowplow-hadoop-etl-0.14.2.jar",
      :shred    => "s3://hadoop-assets/3-enrich/scala-hadoop-shred/snowplow-hadoop-shred-0.3.0.jar",
      :elasticsearch => "s3://hadoop-assets/4-storage/hadoop-elasticsearch-sink/hadoop-elasticsearch-sink-0.1.0.jar"
    }
  end

  it 'knows what the ndjson/urbanairship collector format looks like' do
    EmrJob.is_ua_ndjson("ndjson/com.urbanairship.connect/v1").should eql true
    EmrJob.is_ua_ndjson("thrift").should eql false
    EmrJob.is_ua_ndjson("ndjson/com.somethingelse/v1").should eql false
    EmrJob.is_ua_ndjson("ndjson/com.urbanairship.connect/").should eql false # invalid without version
  end

end
