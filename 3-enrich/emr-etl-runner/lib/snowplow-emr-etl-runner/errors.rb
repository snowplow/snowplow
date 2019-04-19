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

# All errors
module Snowplow
  module EmrEtlRunner

    # The base error class for all <tt>EmrEtlRunner</tt> error classes.
    class Error < StandardError
    end

    # Raised when there's a problem with the supplied configuration (either command line or in configuration file)
    class ConfigError < Error
    end

    # Problem when running Amazon EMR (e.g. job failed)
    class EmrExecutionError < Error
    end

    # Problem when discovering Amazon EMR JobFlow (e.g. cluster is RUNNING rather than WAITING)
    class EmrClusterStateError < Error
    end

    # Problem when discovering Amazon EMR JobFlow (e.g. multiple job-flows discovered with the same name)
    class EmrDiscoveryError < Error
    end

    # A bootstrap failure indicates the job can be safely retried
    class BootstrapFailureError < EmrExecutionError
    end

    # Raised if a directory is not empty
    class DirectoryNotEmptyError < Error
    end

    # Raised if there is no data to process
    # Not strictly an error, but used for control flow
    class NoDataToProcessError < Error
    end

    # Raised if EMR/S3 has unexpected state
    # Requires manual intervention
    class UnexpectedStateError < Error
    end

    # Raised if the linter encountered an issue
    class LinterError < Error
      def initialize(message)
        super(message)
      end
    end

    # Raised if the lock is held
    class LockHeldError < Error
      def initialize(message)
        super(message)
      end
    end
  end
end
