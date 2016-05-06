"""
    Copyright (c) 2016 Snowplow Analytics Ltd. All rights reserved.
    This program is licensed to you under the Apache License Version 2.0,
    and you may not use this file except in compliance with the Apache License
    Version 2.0. You may obtain a copy of the Apache License Version 2.0 at
    http://www.apache.org/licenses/LICENSE-2.0.
    Unless required by applicable law or agreed to in writing,
    software distributed under the Apache License Version 2.0 is distributed on
    an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
    express or implied. See the Apache License Version 2.0 for the specific
    language governing permissions and limitations there under.
    Authors: Fred Blundun
    Copyright: Copyright (c) 2016 Snowplow Analytics Ltd
    License: Apache License Version 2.0
"""

from datetime import date

def add_leading_zero(n):
	if n < 10:
		return '0' + str(n)
	else:
		return str(n)

def doit(start_date, end_date):
	y1 = start_date.year
	y2 = end_date.year
	m1 = start_date.month
	m2 = end_date.month
	d1 = start_date.day
	d2 = end_date.day

	y1s = add_leading_zero(y1)
	y2s = add_leading_zero(y2)
	m1s = add_leading_zero(m1)
	m2s = add_leading_zero(m2)
	d1s = add_leading_zero(d1)
	d2s = add_leading_zero(d2)

	output = []

	if y1 == y2:
		if m1 == m2:
			prefix = y1s + '-' + m1s + '-'
			if d1 == d2:
				output.append(prefix + d1s)
			elif d1 < d2:
				if d1s[0] == d2s[0]:
					output.append(prefix + d1s[0] + '[' + d1s[1] + '-' + d2s[1] + ']')
				else:
					output.append(prefix + d1s[0] + '[' + d1s[1] + '-9]')
					output.append(prefix + d2s[0] + '[0-' + d2s[1] + ']')
					if int(d1s[0]) < int(d2s[0]) - 1:
						output.append(prefix + '[' + str(int(d1s[0])+1) + '-' + str(int(d2s[0]) - 1) + ']')

		elif m1 < m2:
			prefix = y1s + '-'

			output.append(prefix + m1s + '-' + d1s[0] + '[' + d1s[1] + '-9]' )
			output.append(prefix + m1s + '-[' + str(int(d1s[0])+1) + '-9]')
			output.append(prefix + m2s + '-[0-' + str(int(d2s[0])-1) + ']')
			output.append(prefix + m2s + '-' + d2s[0] + '[0-' + d2s[1] + ']')

			if m1 < m2 - 1:
				if m1s[0] == m2s[0]:
					output.append(prefix + m1s[0] + '[' + str(int(m1s[1])+1) + '-' +  str(int(m2s[1])-1) + ']')
				else:
					output.append(prefix + m1s[0] + '[' + str(int(m1s[1])+1) + '-9]')
					output.append(prefix + m2s[0] + '[0-' + str(int(m2s[1])-1) + ']')

	elif y1 < y2:
		output.append(y1s + '-' + m1s + '-' + d1s[0] + '[' + d1s[1] + '-' + '9]')
		output.append(y1s + '-' + m1s + '-[' + str(int(d1s[0])+1) + '-9]')
		output.append(y1s + '-' + m1s[0] + '[' + m1s[1] + '-9]')
		output.append(y1s + '-[' + str(int(m1s[0])+1) + '-9]')

		output.append(y2s + '-' + m2s + '-' + d2s[0] + '[0-' + d2s[1] + ']')
		output.append(y2s + '-' + m2s + '-[0-' + str(int(d2s[0])-1) + ']')
		output.append(y2s + '-' + m2s[0] + '[0-' + str(int(m2s[1])) + ']')
		output.append(y2s + '-[0-' + str(int(m2s[0])-1) + ']')

		for year in range(y1 + 1, y2):
			output.append(str(year))

	return output
