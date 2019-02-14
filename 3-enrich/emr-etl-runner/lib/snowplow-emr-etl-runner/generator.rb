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
require 'open-uri'
require 'avro'
require 'json'

# Abstract class defining a generator behavior.
# See implementing classes: EmrClusterGenerator and PlaybookGenerator.
module Snowplow
  module EmrEtlRunner
    module Generator

      include Contracts

      # Print out the Avro record given by create_record to a file using the schema provided by
      # get_schema
      Contract ConfigHash, String, String, String, Bool, Maybe[String], ArrayOf[String] => nil
      def generate(config, resolver_config, version, filename, debug=false, resume_from=nil, enrichments=[])
        json = JSON.parse(resolver_config, {:symbolize_names => true})
        resolver = Iglu::Resolver.parse(json)
        schema_key = get_schema_key(version)
        raw_schema = resolver.lookup_schema(schema_key)
        avro_schema = Avro::Schema.parse(raw_schema.to_json)
        datum = create_datum(config, debug, resume_from, resolver_config, enrichments)
        if Avro::Schema.validate(avro_schema, datum)
          json = {
            "schema" => schema_key.as_uri,
            "data" => datum
          }.to_json
          File.open(filename, 'w+') { |file|
            file.write(json)
          }
        else
          raise ConfigError, "Config could not be validated against the schema #{avro_schema}"
        end
        nil
      end

      # Get the associated Avro schema name
      Contract String => Iglu::SchemaKey
      def get_schema_key(version)
        raise RuntimeError, '#get_schema_key needs to be defined in all generators.'
      end

      # Create a valid Avro datum
      Contract ConfigHash, Bool, Maybe[String], String, ArrayOf[String] => Hash
      def create_datum(config, debug=false, resume_from=nil, resolver='', enrichments=[])
        raise RuntimeError, '#create_datum needs to be defined in all generators.'
      end

    end
  end
end
