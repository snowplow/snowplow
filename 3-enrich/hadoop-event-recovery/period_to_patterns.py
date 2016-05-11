#!/usr/bin/python

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

from datetime import datetime, date
import sys

def get_patterns(start, end):
	'''
	e.g. start = [1, 6, 0, 3, 2, 9]
	e.g. end = [1, 6, 0, 9, 0, 8]
	'''
	divergent_position = 0
	while divergent_position < len(start) and start[divergent_position] == end[divergent_position]:
		divergent_position += 1
	prefix = start[:divergent_position]
	low_suffix = everything_above(start[divergent_position + 1 :])
	high_suffix = everything_below(end[divergent_position + 1 :])
	divergent_low = start[divergent_position]
	divergent_high = end[divergent_position]
	output = [[divergent_low] + p for p in low_suffix] + [[divergent_high] + p for p in high_suffix]
	if high_suffix:
		divergent_high -= 1
	if low_suffix:
		divergent_low += 1
	if divergent_high >= divergent_low:
		output.append([DigitRange(divergent_low, divergent_high)])
	return [prefix + x for x in output]

def doit(start, end):
	patterns = get_patterns(start, end)
	for p in patterns:
		if len(p) >= 5:
			p.insert(4, '-')
		if len(p) >= 3:
			p.insert(2, '-')
		print(make_pattern(p))

def everything_below(start):
	output = []
	while start and start[-1] == 9:
		start.pop()
	if start:
		last = start.pop()
		output.append(start + [DigitRange(0, last)])
		while start:
			last = start.pop()
			if last > 0:
				output.append(start + [DigitRange(0, last - 1)])
	return output

def everything_above(start):
	output = []
	while start and start[-1] == 0:
		start.pop()
	if start:
		last = start.pop()
		output.append(start + [DigitRange(last, 9)])
		while start:
			last = start.pop()
			output.append(start + [DigitRange(last + 1, 9)])
	return output

def make_pattern(start):
	return ''.join(map(str, start))

class DigitRange(object):
	def __init__(self, low, high):
		self.low = low
		self.high = high
	def to_pattern(self):
		if self.low == self.high:
			return str(self.low)
		elif self.low == self.high - 1:
			return '[' + str(self.low) + str(self.high) + ']'
		else:
			return '[' + str(self.low) + '-' + str(self.high) + ']'
	def __str__(self):
		return self.to_pattern()

def date_component_to_array(component):
	if component < 10:
		return [0, component]
	else:
		return list(map(int, str(component % 100)))

def date_to_array(input_date):
	return [digit for component in [input_date.year, input_date.month, input_date.day] for digit in date_component_to_array(component)]

def main():
	start_date = datetime.strptime(sys.argv[1], '%Y-%m-%d').date()
	end_date = datetime.strptime(sys.argv[2], '%Y-%m-%d').date()
	start = date_to_array(start_date)
	end = date_to_array(end_date)
	doit(start, end)

if __name__ == '__main__':
	main()
