#!/usr/bin/ruby

# This script extracts storage targets from old-style (pre-R88) Snowplow config.yml
# and writes them as new-style self-describing JSONs
#
# Usage:
#   $ ./convert_targets.rb /path/to/config.yml

require 'yaml'
require 'json'


KNOWN_TYPES = ['redshift', 'postgres', 'elasticsearch']


# Read YAML file and extract all targets as array
def get_targets(path)
  config = YAML.load(File.read(path))
  targets = config['storage']['targets'].group_by { |key| key['type'] }
  targets_valid?(targets)
  process_targets(targets)
end

# Write JSON files to current directory
def make_files(targets)
  file_pairs = targets.map { |t| 
    filename = sanitize_filename(t[:data][:name] + '.json')
    [filename, t]
  }
  unique_pairs = uniqufy(file_pairs)
  unique_pairs.each do |name, content| 
    puts "Write #{File.absolute_path(name)}"
    File.open(name, 'w') { |file| file.write(JSON.pretty_generate(content)) }
  end
end

# Accept list list of key/value pairs and append `_1`, `_2` etc to all non-unique pairs
def uniqufy(original_list)
    # Recursive function to bump index unti it is not unique
    def increment(key, acc) if acc.map { |p| p[0] }.include?(key) then increment(bump(key), acc) else key end end          
    def bump(pair) [pair[0], pair[1] + 1] end           # Increment index in pair of key and index

    list = original_list.map { |k, v| [[k, 0], v] }     # Index everything with zero first
    pair_list = list.reduce([]) do |acc, pair|
      acc.unshift([increment(pair[0], acc), pair[1]])   # Increment indexes while they're not unique
    end

    pair_list.map do |key, val|                         # Append non-zero indexes as _N postfix
      if key[1] == 0 then [key[0], val] else ["#{key[0]}_#{key[1]}", val] end
    end
end

# Convert string to valid filename
def sanitize_filename(filename)
  fn = filename.split(/(?<=.)\.(?=[^.])(?!.*\.[^.])/m)
  fn.map! { |s| s.gsub /[^a-z0-9\-]+/i, '_' }
  return fn.join '.'
end


# Check that targets have valid type. Throw exception otherwise
def targets_valid?(targets_hash)
  targets_hash.keys.each { |type|
    unless KNOWN_TYPES.include? type
      raise "Unknown target type [#{type}]"
    end
  }
end

# Convert each target to self-describing JSON accroding to its type
def process_targets(targets_hash)
  redshift = targets_hash['redshift']
  postgres = targets_hash['postgres']
  elasticsearch = targets_hash['elasticsearch']

  redshift.to_a.map { |t| process_redshift(t) } + postgres.to_a.map { |t| process_postgres(t) } + elasticsearch.to_a.map { |t| process_elasticsearch(t) }
end

# Generate Redshift target from YAML configuration
def process_redshift(target)
  {
    :schema => 'iglu:com.snowplowanalytics.snowplow.storage/redshift_config/jsonschema/1-0-0',
    :data => {
        :name => target['name'],
        :host => target['host'],
        :database => target['database'],
        :port => target['port'],
        :sslMode => target['ssl_mode'].upcase,
        :schema => target['table'].split('.')[0],
        :username => target['username'],
        :password => target['password'],
        :maxError => target['maxerror'],
        :compRows => target['comprows'],
        :purpose => 'ENRICHED_EVENTS'
    }
  }
end

# Generate Postgres target from YAML configuration
def process_postgres(target)
  {
    :schema => 'iglu:com.snowplowanalytics.snowplow.storage/postgresql_config/jsonschema/1-0-0',
    :data => {
        :name => target['name'],
        :host => target['host'],
        :database => target['database'],
        :port => target['port'],
        :sslMode => target['ssl_mode'].upcase,
        :schema => target['table'].split('.')[0],
        :username => target['username'],
        :password => target['password'],
        :purpose => 'ENRICHED_EVENTS'
    }
  }
end

# Generate Elasticsearch target from YAML configuration
def process_elasticsearch(target)
  {
    :schema => 'iglu:com.snowplowanalytics.snowplow.storage/elastic_config/jsonschema/1-0-0',
    :data => {
        :name => target['name'],
        :host => target['host'],
        :index => target['database'],
        :port => target['port'],
        :type => target['table'],
        :nodesWanOnly => target['es_nodes_wan_only'],
        :purpose => 'FAILED_EVENTS'
    }
  }
end


if __FILE__ == $0
  if ARGV.length != 1
    puts 'Script accepts exactly one argument - path to config.yml file'
  elsif not File.readable?(ARGV[0])
    puts "File #{ARGV[0]} cannot be accessed"
  else
    targets = get_targets(ARGV[0])
    make_files(targets)
    puts "All targets JSONs created. Don't forget to remove them from #{ARGV[0]}"
  end
end
