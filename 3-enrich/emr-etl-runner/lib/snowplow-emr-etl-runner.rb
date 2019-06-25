# Copyright (c) 2012-2019 Snowplow Analytics Ltd. All rights reserved.
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
# Copyright:: Copyright (c) 2012-2019 Snowplow Analytics Ltd
# License::   Apache License Version 2.0

# Ruby 1.9.2 onwards doesn't add . into $LOAD_PATH by default - use require_relative instead
require_relative 'snowplow-emr-etl-runner/errors'
require_relative 'snowplow-emr-etl-runner/contracts'
require_relative 'snowplow-emr-etl-runner/utils'
require_relative 'snowplow-emr-etl-runner/monitoring/logging'
require_relative 'snowplow-emr-etl-runner/monitoring/snowplow'
require_relative 'snowplow-emr-etl-runner/cli'
require_relative 'snowplow-emr-etl-runner/job_result'
require_relative 'snowplow-emr-etl-runner/s3'
require_relative 'snowplow-emr-etl-runner/emr'
require_relative 'snowplow-emr-etl-runner/emr_job'
require_relative 'snowplow-emr-etl-runner/lock/lock'
require_relative 'snowplow-emr-etl-runner/lock/file_lock'
require_relative 'snowplow-emr-etl-runner/lock/consul_lock'
require_relative 'snowplow-emr-etl-runner/runner'
require_relative 'snowplow-emr-etl-runner/generator'
require_relative 'snowplow-emr-etl-runner/emr_config_generator'
require_relative 'snowplow-emr-etl-runner/emr_playbook_generator'
require_relative 'snowplow-emr-etl-runner/linter'


module Snowplow
  module EmrEtlRunner
    NAME    = "snowplow-emr-etl-runner"
    VERSION = "0.35.0"
  end
end
