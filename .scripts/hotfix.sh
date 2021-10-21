#!/usr/bin/env bash

#
# Copyright (c) 2021 Snowplow Analytics Ltd. All rights reserved.
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
# USAGE: ./commit_retag.sh $submod_name $commit_msg $do_retag $tag_name $prev_sha
# - the submodule name
# - the commit message for update
# - flag, whether to do retagging (i.e. delete tag and retag on the update commit)
# - the tag name to be deleted and used to retag the update commit
# - the superproject commit that the tag referred to previously (for sanity check)
#
# Assumes tag_name and prev_sha are non empty.
#

REPO_REMOTE='origin'

# -----------------------------------------------------------------------------
#  FUNCTIONS & PROCEDURES
# -----------------------------------------------------------------------------
. ${0%/*}/lib.sh

# Given a submodule name and a commit message,
#  - updates the submodule to its **latest** commit (default branch)
#  - and commits onto the superproject using the given commit message
# Fails if:
#  - no submodule path was found for given name
#  - no changes after update (submodule already on latest)
function commit_update() {
    [ "$#" -eq 2 ] || die "2 arguments required, $# provided to 'func:commit_update'"
    local __mod_name="$1"
    local __commit_msg="$2"

    submodule_path=`git submodule--helper config submodule."${__mod_name}".path`
    [ -z "${submodule_path}" ] && die "Exiting: Submodule path not found for ${__mod_name}"

    git submodule update --remote --checkout "${__mod_name}"
    [[ -z `git status -s` ]] && die 'Exiting: No unstaged changes found!'

    git add "${submodule_path}"
    git commit -m "${__commit_msg}"
}

# Given a tag and a commit hash,
#  - deletes the tag remotely and locally
# Fails if:
#  - the tag does not exist
#  - the tag does not reference the given commit
function delete_tag() {
    [ "$#" -eq 2 ] || die "2 arguments required, $# provided to 'func:delete_tag'"
    local __tag_name="$1"
    local __commit_sha="$2"

    if tag_ref=`git rev-parse --quiet --verify --end-of-options "${__tag_name}"^{commit}` ; then
        if [ "${tag_ref}" != "${__commit_sha}" ] ; then
            die "Exiting: Tag ${__tag_name} is not associated to specified commit ${__commit_sha}"
        fi
    else
        die "Exiting: Tag ${__tag_name} not found"
    fi

    echo 'Tag reference ok.'
    echo 'Deleting remote tag..'
    git push --delete "${REPO_REMOTE}" "${__tag_name}"
    echo 'Deleting local tag..'
    git tag --delete "${__tag_name}"
}

# -----------------------------------------------------------------------------
#  ARGUMENT PARSING
# -----------------------------------------------------------------------------
[ "$#" -eq 5 ] || die "5 arguments required, $# provided to 'script:hotfix'"
submod_name="$1"
commit_msg="$2"
do_retag="$3"
tag_name="$4"
prev_sha="$5"

# -----------------------------------------------------------------------------
#  COMMANDS
# -----------------------------------------------------------------------------

# Update the submodule and commit changes
commit_update "${submod_name}" "${commit_msg}"

echo 'LOG: git-show'
git show --format=raw HEAD^..HEAD

# do_retag flag set to yes, first delete and then tag the latest local commit
if [ "${do_retag}" = 'yes' ]; then
    delete_tag "${tag_name}" "${prev_sha}"
    echo 'Tagging latest commit..'
    git tag -a "${tag_name}" -m "${tag_name}"
fi
