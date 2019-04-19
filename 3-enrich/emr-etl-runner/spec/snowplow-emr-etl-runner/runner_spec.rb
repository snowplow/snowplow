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

require 'spec_helper'

Runner = Snowplow::EmrEtlRunner::Runner

describe Runner do

  default = {
    :staging => true,
    :enrich => true,
    :staging_stream_enrich => false,
    :shred => true,
    :es => true,
    :archive_raw => true,
    :rdb_load => true,
    :consistency_check => true,
    :load_manifest_check => true,
    :analyze => true,
    :archive_enriched => true,
    :archive_shredded => true
  }

  default_rdb_loader_steps = {
    :skip => [],
    :include => []
  }

  describe '#get_steps' do
    it 'adds default steps without resume, skip and enrich_mode' do
      expect(Runner.get_steps([], nil, nil)).to eq(default)
    end

    it 'skips staging, enrich and archive_raw in stream enrich mode' do
      expected = default.clone
      expected[:staging] = false
      expected[:staging_stream_enrich] = true
      expected[:enrich] = false
      expected[:archive_raw] = false
      expect(Runner.get_steps([], nil, "s3://foo")).to eq(expected)
    end

    it 'skips staging, enrich, staging_stream_enrich and archive_raw in stream enrich mode with resume-from shred' do
      expected = default.clone
      expected[:staging] = false
      expected[:enrich] = false
      expected[:archive_raw] = false
      expect(Runner.get_steps([], "shred", "s3://foo")).to eq(expected)
    end

    it 'skips staging, enrich, staging_stream_enrich and archive_raw in stream enrich mode with resume-from shred' do
      expected = default.clone
      expected[:staging] = false
      expected[:enrich] = false
      expected[:archive_raw] = false
      expect(Runner.get_steps([], "shred", "s3://foo")).to eq(expected)
    end

    it 'skips only steps that were specified' do
      expected = default.clone
      expected[:rdb_load] = false
      expected[:archive_raw] = false
      expected[:shred] = false
      expect(Runner.get_steps(['rdb_load','archive_raw', 'shred'], nil, nil)).to eq(expected)
    end

    it 'skipping staging and enrich in stream mode doesn\'t have any effect' do
      with_skip = Runner.get_steps(['staging', 'enrich'], nil, "s3://foo")
      without_skip = Runner.get_steps([], nil, "s3://foo")
      expect(with_skip).to eq(without_skip)
    end
  end

  describe '#get_rdbloader_steps' do
    it 'adds default steps without with default EER steps' do
      expect(Runner.get_rdbloader_steps(default, [])).to eq(default_rdb_loader_steps)
    end
  end
end
