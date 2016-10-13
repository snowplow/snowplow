require 'zlib'
require 'csv'
require 'tzinfo'
require 'date'
require 'json'

# Ruby module to support the load of Snowplow events into BigQuery.
module Snowplow
  module StorageLoader
    module BigQueryLoader

      EVENT_FILES = "part-*"
      EVENT_FIELD_SEPARATOR = "\t"
      FIELD_NAMES = %w(app_id platform etl_tstamp collector_tstamp dvce_tstamp event event_id txn_id name_tracker v_tracker v_collector v_etl user_id user_ipaddress user_fingerprint domain_userid domain_sessionidx network_userid geo_country geo_region geo_city geo_zipcode geo_latitude geo_longitude geo_region_name ip_isp ip_organization ip_domain ip_netspeed page_url page_title page_referrer page_urlscheme page_urlhost page_urlport page_urlpath page_urlquery page_urlfragment refr_urlscheme refr_urlhost refr_urlport refr_urlpath refr_urlquery refr_urlfragment refr_medium refr_source refr_term mkt_medium mkt_source mkt_term mkt_content mkt_campaign contexts se_category se_action se_label se_property se_value unstruct_event tr_orderid tr_affiliation tr_total tr_tax tr_shipping tr_city tr_state tr_country ti_orderid ti_sku ti_name ti_category ti_price ti_quantity pp_xoffset_min pp_xoffset_max pp_yoffset_min pp_yoffset_max useragent br_name br_family br_version br_type br_renderengine br_lang br_features_pdf br_features_flash br_features_java br_features_director br_features_quicktime br_features_realplayer br_features_windowsmedia br_features_gears br_features_silverlight br_cookies br_colordepth br_viewwidth br_viewheight os_name os_family os_manufacturer os_timezone dvce_type dvce_ismobile dvce_screenwidth dvce_screenheight doc_charset doc_width doc_height tr_currency tr_total_base tr_tax_base tr_shipping_base ti_currency ti_price_base base_currency geo_timezone mkt_clickid mkt_network etl_tags dvce_sent_tstamp refr_domain_useri refr_dvce_tstamp derived_contexts domain_sessionid derived_tstamp event_vendor event_name event_format event_version event_fingerprint true_tstamp)
      BQ_CMD = 'bq load --ignore_unknown_values --max_bad_records=%d %s --source_format=NEWLINE_DELIMITED_JSON --schema=%s %s.%s %s'

      # Process Snowplow enriched files into daily partition file.
      #
      # Parameters:
      # +events_dir+:: the directory holding the event files to load 
      # +target+:: the configuration options for this target
      def self.process_events(events_dir, target, snowplow_tracking_enabled)
        puts "Processing Snowplow events into daily tables"
        event_files = get_event_files(events_dir)

        line_count = 0
        day_stats = {}
        out_files = {}

        event_files.each do |path|
          puts "Processing #{path}..."

          options = {
            :col_sep => EVENT_FIELD_SEPARATOR,
            :headers => FIELD_NAMES,
            :force_quotes => true,
            :encoding => "utf-8",
            :quote_char => "\x00"
          }

          file = Zlib::GzipReader.open(path)

          csv = CSV.new(file, options)
          csv.each do |row|
            line_count += 1
            collector_tstamp = row["collector_tstamp"]
            tz = TZInfo::Timezone.get(target[:timezone])

            utc = DateTime.strptime(collector_tstamp, '%Y-%m-%d %H:%M:%S')
            local_time = tz.utc_to_local(utc)
            local_date = local_time.strftime("%Y-%m-%d")

            if day_stats.key?(local_date)
              day_stats[local_date] += 1
            else
              day_stats[local_date] = 1
            end

            out = {}
            out["_unstruct_event"] = {}

            row.each do |column, value|
              out[column] = value
              if column != "unstruct_event"
                next
              end

              if not value
                 out[column] = "{}"
                 next
              end

              val_obj = JSON.load(value)

              if val_obj.key?("data")
                schema_name = val_obj["data"]["schema"].match(/iglu:[^\/]+\/([^\/]+)\/jsonschema\/[0-9]+-[0-9]+-[0-9]+/).captures
                
                if !schema_name
                  raise "Error regex failed!"
                end
                
                schema_name = schema_name[0]
                
                out["_unstruct_event"][schema_name] = val_obj["data"]["data"]
                out["_unstruct_event"][schema_name]["_schema"] = val_obj["data"]["schema"]
              end
            end

            if not out_files.key?(local_date)
              out_file = File.join(target[:processing_dir], "#{local_date}.gzip")
              out_files[local_date] = Zlib::GzipWriter.new(File.open(out_file, 'a+'))
            end

            out_files[local_date].puts(JSON.dump(out))
          end
          csv.close()
        end

        out_files.each do |key, out_file|
          out_file.close()
        end

        puts "Line count #{line_count}"
        puts "Day stats #{day_stats}"

      end

      # Loads the Snowplow event files into BigQuery.
      #
      # Parameters:
      # +target+:: the configuration options for this target
      def self.load_events(target, snowplow_tracking_enabled)
        puts "Loading Snowplow events into #{target[:name]} (BigQuery)..."

        table_files = Dir[File.join(target[:processing_dir], '*.gzip')].select { |f| File.file?(f) }

        replace = ''
        if target[:replace]
          replace = '--replace'
        end

        table_files.each do |file|
          table = target[:table] + File.basename(file, ".gzip").gsub('-', '')

          cmd = BQ_CMD % [target[:maxerror], replace, target[:schema], target[:database], table, file]
          puts cmd

          fd = IO.popen(cmd)
          puts(fd.readlines)
          
        end

        if snowplow_tracking_enabled
          Monitoring::Snowplow.instance.track_load_succeeded()
        end
      end

      private

      # Return the list of event files.
      #
      # Parameters:
      # +events_dir+:: the directory holding the event files to load 
      #
      # Returns the array of cold files
      def self.get_event_files(events_dir)

         Dir[File.join(events_dir, '**', EVENT_FILES)].select { |f|
          File.file?(f) # In case of a dir ending in .tsv
        }
      end

    end
  end
end
