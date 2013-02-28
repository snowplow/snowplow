# Copyright (c) 2012-2013 SnowPlow Analytics Ltd. All rights reserved.
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
# Copyright:: Copyright (c) 2012-2013 SnowPlow Analytics Ltd
# License::   Apache License Version 2.0

require 'infobright-loader'

# Ruby module to support the load of SnowPlow events into Infobright.
module SnowPlow
  module StorageLoader
    module IceLoader # Called IceLoader to prevent clash with external InfobrightLoader module

      # Constants for the load process
      EVENT_FIELD_SEPARATOR = "\t"
      EVENT_FIELD_ENCLOSER  = "NULL" # Explicitly specify no encloser. Empty quotes won't work. See https://github.com/snowplow/snowplow/issues/88 for details

      # Loads the SnowPlow event files into Infobright.
      #
      # Parameters:
      # +config+:: the hash of configuration options 
      def load_events(config)
        puts "Loading SnowPlow events into Infobright..."

        # Configuration for our database
        db = InfobrightLoader::Db::DbConfig.new(
               config[:storage][:database],
               config[:storage][:username],
               config[:storage][:password])

        # Load the events table from our download folder
        begin
          failed_files = InfobrightLoader::Loader::load_from_folder(
            config[:download][:folder],      # Folder
            config[:storage][:table],        # Table
            db,                              # Database config
            EVENT_FIELD_SEPARATOR,           # Field separator
            EVENT_FIELD_ENCLOSER             # Field encloser
          )
          if failed_files.any?
            raise DatabaseLoadError, "The following files could not be loaded:\n" + failed_files.join("\n")
          end
        rescue InfobrightLoader::Loader::LoadError => le
          # Re-raise as a StorageLoader own-brand exception
          raise DatabaseLoadError, le.message
        end

        # Now delete the local files
        delete_events(config[:download][:folder])
      end
      module_function :load_events

      private

      # Empties the download folder now that the events
      # have been loaded
      #
      # Parameters:
      # +folder+:: the folder containing the files to delete 
      def delete_events(folder)
        FileUtils.rm_rf("#{folder}/.", :secure => true)
      end
      module_function :delete_events

    end
  end
end