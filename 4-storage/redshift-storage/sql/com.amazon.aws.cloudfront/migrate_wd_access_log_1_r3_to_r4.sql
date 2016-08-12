-- Copyright (c) 2015 Snowplow Analytics Ltd. All rights reserved.
--
-- This program is licensed to you under the Apache License Version 2.0,
-- and you may not use this file except in compliance with the Apache License Version 2.0.
-- You may obtain a copy of the Apache License Version 2.0 at http://www.apache.org/licenses/LICENSE-2.0.
--
-- Unless required by applicable law or agreed to in writing,
-- software distributed under the Apache License Version 2.0 is distributed on an
-- "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
-- See the Apache License Version 2.0 for the specific language governing permissions and limitations there under.
--
-- Version:     Ports 1-0-3 to 1-0-4
-- URL:         -
--
-- Authors:     Fred Blundun
-- Copyright:   Copyright (c) 2015 Snowplow Analytics Ltd
-- License:     Apache License Version 2.0

BEGIN TRANSACTION;

ALTER TABLE atomic.com_amazon_aws_cloudfront_wd_access_log_1
	RENAME COLUMN x_edge_request_type TO x_edge_request_id;

ALTER TABLE atomic.com_amazon_aws_cloudfront_wd_access_log_1	
	ADD COLUMN x_forwarded_for varchar(45) encode lzo;

ALTER TABLE atomic.com_amazon_aws_cloudfront_wd_access_log_1
	ADD COLUMN ssl_protocol varchar(32) encode lzo;

ALTER TABLE atomic.com_amazon_aws_cloudfront_wd_access_log_1
	ADD COLUMN ssl_cipher varchar(64) encode lzo;

ALTER TABLE atomic.com_amazon_aws_cloudfront_wd_access_log_1
	ADD COLUMN x_edge_response_result_type varchar(32) encode lzo;

END TRANSACTION;
