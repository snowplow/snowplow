#!/usr/bin/env bash

#
# Copyright (c) 2022 Snowplow Analytics Ltd. All rights reserved.
#
# This program is licensed to you under the Apache License Version 2.0,
# and you may not use this file except in compliance with the Apache License Version 2.0.
# You may obtain a copy of the Apache License Version 2.0 at http://www.apache.org/licenses/LICENSE-2.0.
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the Apache License Version 2.0 is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the Apache License Version 2.0 for the specific language governing permissions and limitations there under.
#


set -e

#
# USAGE: ./per_module.sh $name $sm_path $sha1 $toplevel
# This script is to be run as the `git submodule foreach` <command>.
# Its arguments are:
# - the name of the relevant submodule section in .gitmodules
# - the path of the submodule as recorded in the immediate superproject
# - the commit as recorded in the immediate superproject
# - the absolute path to the top-level of the immediate superproject
#

DEF_REMOTE='origin'

# -----------------------------------------------------------------------------
#  FUNCTIONS & PROCEDURES
# -----------------------------------------------------------------------------
. ${0%/*}/lib.sh

# -----------------------------------------------------------------------------
#  ARGUMENT PARSING
# -----------------------------------------------------------------------------
[ "$#" -eq 4 ] || die "4 arguments required, $# provided to 'script:per_module'"
mod_name="$1"
rel_path="$2"
mod_sha1="$3"
top_level="$4"

# -----------------------------------------------------------------------------
#  COMMANDS
# -----------------------------------------------------------------------------
REMOTE_HEAD_SYMREF=`git symbolic-ref --short "refs/remotes/${DEF_REMOTE}/HEAD"`
DEF_BRANCH="${REMOTE_HEAD_SYMREF##*/}"

git fetch --tags --quiet "${DEF_REMOTE}" "${DEF_BRANCH}"

while read -r commit_sha1
do

    while read -r tag
    do

        if r_tag=`is_release_tag "${tag}" "${mod_name}"` ; then
            merge_bump "${top_level}" "${mod_name}" "${rel_path}" "${commit_sha1}" "${r_tag}"
            break
        fi

    done < <(git tag --points-at "${commit_sha1}")

done < <(git rev-list "${mod_sha1}"..FETCH_HEAD --simplify-by-decoration --reverse)
