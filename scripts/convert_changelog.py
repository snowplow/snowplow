#!/usr/bin/env python

# Copyright (c) 2016 Snowplow Analytics Ltd. All rights reserved.
# This program is licensed to you under the Apache License Version 2.0,
# and you may not use this file except in compliance with the Apache License Version 2.0.
# You may obtain a copy of the Apache License Version 2.0 at http://www.apache.org/licenses/LICENSE-2.0.
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the Apache License Version 2.0 is distributed on
# an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
# express or implied. See the Apache License Version 2.0 for the specific
# language governing permissions and limitations there under.

# Script to convert CHANGELOG to markdown bullet points with a heading for each subproject
# Sample usage: < CHANGELOG ./convert_changelog

import fileinput
import sys
import re

subproject = None
ignored_lines = 0
for line in fileinput.input():
    if line == "\n":
        break
    # Ignore the first two lines
    if ignored_lines < 2:
        ignored_lines += 1
        continue
    new_subproject, contents = line.split(": ", 1)
    # We have moved onto tickets for a new subproject
    if new_subproject != subproject:
        sys.stdout.write("\n### " + new_subproject + "\n\n")
        subproject = new_subproject
    contents_chars = list(contents)
    contents_chars[0] = contents_chars[0].capitalize()
    sys.stdout.write("* " + "".join(contents_chars))
