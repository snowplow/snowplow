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

# Author::    Ben Fradet (mailto:support@snowplowanalytics.com)
# Copyright:: Copyright (c) 2012-2019 Snowplow Analytics Ltd
# License::   Apache License Version 2.0

require 'contracts'
require 'iglu-client'
require 'json'

# Class linting Iglu config as well as enrichments
module Snowplow
  module EmrEtlRunner
    class Linter

      attr_reader :resolver

      include Contracts

      # Creates a Linter from an Iglu resolver
      Contract Iglu::Resolver => Linter
      def initialize(resolver)
        @resolver = resolver
        self
      end

      # Lints an Iglu config by trying to parse the resolver config
      Contract String => Or[Linter, LinterError]
      def self.lint(resolver_config)
        begin
          json = JSON.parse(resolver_config, {:symbolize_names => true})
          resolver = Iglu::Resolver.parse(json)
          Linter.new(resolver)
        rescue Iglu::IgluError => e
          LinterError.new("Invalid resolver config #{resolver_config}: #{e.message}")
        rescue JSON::ParserError => e
          LinterError.new("Could not parse resolver config #{resolver_config}: #{e.message}")
        end
      end

      # Lints enrichments with the help of Iglu
      Contract ArrayOf[String] => ArrayOf[LinterError]
      def lint_enrichments(enrichments)
        res = enrichments.flat_map do |enrichment|
          begin
            json = JSON.parse(enrichment, {:symbolize_names => true})
            @resolver.validate(json)
            []
          rescue Iglu::IgluError, JSON::ParserError => e
            [ LinterError.new("Could not validate enrichment #{enrichment}: #{e.message}") ]
          end
        end
      end

    end
  end
end
