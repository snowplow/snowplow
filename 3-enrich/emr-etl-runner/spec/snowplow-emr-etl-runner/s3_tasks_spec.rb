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

S3Tasks = Snowplow::EmrEtlRunner::S3Tasks

describe S3Tasks do

  region = 'us-west-1'

  examples = [
    # spec                               # filepath                                                                    # basename                                                         # expected
    [ 'CloudFront log in bucket root'    , 'EO3VS2J9DJRJR.2015-03-01-23.6e20d524.gz'                                   , 'EO3VS2J9DJRJR.2015-03-01-23.6e20d524.gz'                        , 'EO3VS2J9DJRJR.2015-03-01-23.6e20d524.gz'                                                ] ,
    [ 'CloudFront log in sub-folder'     , 'log/EO3VS2J9DJRJR.2015-03-01-23.6e20d524.gz'                               , 'EO3VS2J9DJRJR.2015-03-01-23.6e20d524.gz'                        , "EO3VS2J9DJRJR.2015-03-01-23.6e20d524.#{region}.log.gz"                                  ] ,
    [ 'tomcat7 log'                      , 'i-615f3c85/_var_log_tomcat7_localhost_access_log.txt-1426089661.gz'        , '_var_log_tomcat7_localhost_access_log.txt-1426089661.gz'        , 'var_log_tomcat7_localhost_access_log.2015-03-11-16.us-west-1.i-615f3c85.txt.gz'         ] ,
    [ 'tomcat7_rotated log'              , 'i-615f3c85/_var_log_tomcat7_rotated_localhost_access_log.txt1426089661.gz' , '_var_log_tomcat7_rotated_localhost_access_log.txt1426089661.gz' , 'var_log_tomcat7_rotated_localhost_access_log.2015-03-11-16.us-west-1.i-615f3c85.txt.gz' ] ,
    [ 'tomcat8_rotated log'              , 'i-615f3c85/_var_log_tomcat8_rotated_localhost_access_log.txt1426089661.gz' , '_var_log_tomcat8_rotated_localhost_access_log.txt1426089661.gz' , 'var_log_tomcat8_rotated_localhost_access_log.2015-03-11-16.us-west-1.i-615f3c85.txt.gz' ] ,
    [ 'unknown .gz in root'              , 'unknown.gz'                                                                , 'unknown.gz'                                                     , 'unknown.gz'                                                                             ] ,
    [ 'unknown .gz in sub-folder'        , 'foo/bar/unknown.gz'                                                        , 'unknown.gz'                                                     , "unknown.#{region}.bar.gz"                                                               ] , 
    [ 'unknown .gz with _ in root'       , ''                                                                          , '_unknown.gz'                                                    , 'unknown.gz'                                                                             ] ,  
    [ 'unknown .gz with _ in sub-folder' , 'foo/bar/_unknown.gz'                                                       , '_unknown.gz'                                                    , "_unknown.#{region}.bar.gz"                                                              ] ,        
  ]

  fix_filenames = S3Tasks.build_fix_filenames(region)

  examples.each do |eg|
    spec = eg[0]
    filepath = eg[1]
    basename = eg[2]
    expected = eg[3]

    it "should return #{expected} when filepath == #{filepath} and basename == #{basename} for a #{spec}" do
      fix_filenames.call(basename, filepath).should eql expected
    end
  end

end
