# Copyright (c) 2012-2016 Snowplow Analytics Ltd. All rights reserved.
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
# Copyright:: Copyright (c) 2012-2014 Snowplow Analytics Ltd
# License::   Apache License Version 2.0

require 'optparse'
require 'date'
require 'base64'
require 'yaml'
require 'erb'
require 'aws-sdk'
require 'sluice'
require 'json'
require 'iglu-client'

# Config module to hold functions related to CLI argument parsing
# and config file reading to support storage loading.
module Snowplow
  module StorageLoader
    module Config

      @@storage_targets = Set.new(%w(redshift_config postgresql_config elasticsearch_config amazon_dynamodb_config))

      # Return the configuration loaded from the supplied YAML file, plus
      # the additional constants above.
      def self.get_config()

        options = Config.parse_args()

        if options[:b64config].nil?
          if Config.indicates_read_from_stdin?(options[:config])
            unsymbolized_config = $stdin.readlines.join
          else
            unsymbolized_config = File.new(options[:config]).read
          end
        else
          unsymbolized_config = Base64.decode64(options[:b64config])
        end

        erb_config = ERB.new(unsymbolized_config).result(binding)

        config = Config.recursive_symbolize_keys(YAML.load(erb_config))

        # Build resolver to validate targets
        resolver = Iglu::Resolver.parse(JSON.parse(File.read(options[:resolver]), {:symbolize_names => true}))
        config[:storage][:targets] = load_storage_targets(options[:targets], resolver)

        # Add in our skip and include settings
        config[:skip] = options[:skip]
        config[:include] = options[:include]

        # Add trailing slashes if needed to the non-nil buckets
        config[:aws][:s3][:buckets] = add_trailing_slashes(config[:aws][:s3][:buckets])

        # Retrieve AWS credentials from EC2 role if necessary
        if config[:aws][:access_key_id] == 'iam' and config[:aws][:secret_access_key] == 'iam'
          credentials_from_role = Aws::InstanceProfileCredentials.new.credentials
          config[:aws][:access_key_id] = credentials_from_role.access_key_id
          config[:aws][:secret_access_key] = credentials_from_role.secret_access_key
        end

        # Add in our comprows setting
        config[:comprows] = options[:comprows]
        
        config[:storage][:download][:folder] = Sluice::Storage::trail_slash(config[:storage][:download][:folder])

        # Determine whether we need to download events
        config[:download_required] = config[:storage][:targets].count { |t| check_target("postgresql_config").call(t) } > 0

        # If Postgres is the target, check that the download folder exists and is empty
        if config[:download_required]
          # Check that the download folder exists...
          unless File.directory?(config[:storage][:download][:folder])
            raise ConfigError, "Download folder '#{config[:storage][:download][:folder]}' not found"
          end
        
          # ...and it is empty
          unless config[:skip].include?("download")
            if !(Dir.entries(config[:storage][:download][:folder]) - %w{ . .. }).empty?
              raise ConfigError, "Download folder '#{config[:storage][:download][:folder]}' is not empty"
            end
          end
        end

        config
      end  

      # Load configuration in self-describing JSONs from specified path
      # Abort whole program on any error
      def self.load_storage_targets(targets_path, resolver)
        Dir.entries(targets_path).select do |f| 
          not f.start_with?('.') 
        end.map do |f| 
          {:file => f, :json => JSON.parse(File.read(targets_path + '/' + f), {:symbolize_names => true}) }
        end.map do |j|
          begin
            self_describing_json = Iglu::SelfDescribingJson.parse_json(j[:json])
            self_describing_json.validate(resolver)
            j[:json] = self_describing_json
            j
          rescue Exception => e
            print "Error in [#{j[:file]}] "
            puts e.message
            abort("Shutting down")
          end
        end.map do |j|
          target = j[:json].schema.name
          unless @@storage_targets.include?(target)
            print "Error in [#{j[:file]}] "
            puts "StorageLoader doesn't support storage target configuration with name '#{target}'"
            abort("Shutting down")
          end
          j[:json]
        end
      end

      # Return lambda checking that self-describing JSON with target conforms condition
      def self.check_target(name, model=nil, revision=nil)
        lambda do |t|
          t.schema.name == name and (model.nil? or t.schema.version.model == model) and (revision.nil? or t.schema.version.revision == revision)
        end
      end

    private

      # Add trailing slashes
      def self.add_trailing_slashes(bucketsHash)
        with_slashes_added = {}
        for k0 in bucketsHash.keys
          if bucketsHash[k0].class == ''.class
            with_slashes_added[k0] = Sluice::Storage::trail_slash(bucketsHash[k0])
          elsif bucketsHash[k0].class == {}.class
            y = {}
            for k1 in bucketsHash[k0].keys
              y[k1] = Sluice::Storage::trail_slash(bucketsHash[k0][k1])
            end
            with_slashes_added[k0] = y
          else
            with_slashes_added[k0] = nil
          end
        end

        with_slashes_added
      end

      # Parse the command-line arguments
      # Returns: the hash of parsed options[
      def self.parse_args()

        # Handle command-line arguments
        options = {}
        options[:skip] = []
        options[:include] = []
        optparse = OptionParser.new do |opts|

          opts.banner = "Usage: %s [options]" % NAME
          opts.separator ""
          opts.separator "Specific options:"
          opts.on('-c', '--config CONFIG', 'configuration fgle') { |config| options[:config] = config }
          opts.on('-b', '--base64-config-string CONFIG', 'base64-encoded configuration string') { |config| options[:b64config] = config }
          opts.on('-t', '--targets TARGETS_DIR', 'targets directory') { |config| options[:targets] = config }
          opts.on('-i', '--include compupdate,vacuum', Array, 'include optional work step(s)') { |config| options[:include] = config }
          opts.on('-s', '--skip download|delete,load,shred,analyze,archive_enriched', Array, 'skip work step(s)') { |config| options[:skip] = config }
          opts.on('-r', '--resolver RESOLVER', 'Iglu resolver config file') { |config| options[:resolver] = config }

          opts.separator ""
          opts.separator "Common options:"

          opts.on_tail('-h', '--help', 'Show this message') { puts opts; exit }
          opts.on_tail('-v', "--version", "Show version") do
            puts "%s %s" % [NAME, VERSION]
            exit
          end
        end

        # Run OptionParser's structural validation
        begin
          optparse.parse!
        rescue OptionParser::InvalidOption, OptionParser::MissingArgument
          raise ConfigError, "#{$!.to_s}\n#{optparse}"
        end

        # Check our skip argument
        options[:skip].each { |opt|
          unless %w(download delete load shred analyze archive_enriched).include?(opt)
            raise ConfigError, "Invalid option: skip can be 'download', 'delete', 'load', 'analyze' or 'archive_enriched', not '#{opt}'"
          end
        }

        # Check our include argument
        options[:include].each { |opt|
          unless %w(compupdate vacuum).include?(opt)
            raise ConfigError, "Invalid option: include can be 'compupdate' or 'vacuum', not '#{opt}'"
          end
        }

        if options[:config].nil? and options[:b64config].nil?
          raise ConfigError, "Missing option: config or base64-config-string\n#{optparse}"
        end

        if options[:targets].nil?
          raise ConfigError, "Missing option: targets\n#{optparse}"
        end

        # Check the config file exists if config is not read from stdin
        if options[:b64config].nil?
          unless Config.indicates_read_from_stdin?(options[:config]) || File.file?(options[:config])
            raise ConfigError, "Configuration file '#{options[:config]}' does not exist, or is not a file."
          end
        end

        if not File.directory?(options[:targets])
          raise ConfigError, "Targets argument '#{options[:targets]}' does not exist, or is not a directory."
        end

        if options[:resolver].nil? || (not File.readable?(options[:resolver]))
          raise ConfigError, "Resolver config '#{options[:resolver]}' isn't available"
        end

        options
      end

      # A single hyphen indicates that the config should be read from stdin
      def self.indicates_read_from_stdin?(config_option)
        config_option == '-'
      end

      # Convert all keys in arbitrary hash into symbols
      # Taken from http://stackoverflow.com/a/10721936/255627
      def self.recursive_symbolize_keys(h)
        case h
        when Hash
          Hash[
            h.map do |k, v|
              [ k.respond_to?(:to_sym) ? k.to_sym : k, recursive_symbolize_keys(v) ]
            end
          ]
        when Enumerable
          h.map { |v| recursive_symbolize_keys(v) }
        else
          h
        end
      end

    end
  end
end
