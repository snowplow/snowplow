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


# -----------------------------------------------------------------------------
#  FUNCTIONS & PROCEDURES
# -----------------------------------------------------------------------------

function die() {
    echo "$@" 1>&2 ; exit 1;
}

# Given a tag and a module name,
#   if the tag is a release tag for module, echoes a tag
#   else returns 1
# Uses shopt and sets locale, so runs in subshell
function is_release_tag() (
    [ "$#" -eq 2 ] || die "2 arguments required, $# provided to 'func:is_release_tag'"
    local __tag="$1"
    local __module="$2"

    # For extended glob patterns and ensure [0-9] range
    shopt -s extglob
    LC_ALL=C

    case "${__module}" in
        testing/micro)
            tag_pattern='micro-+([0-9]).+([0-9]).+([0-9])'
            [[ "${__tag}" = $tag_pattern ]] && echo "${__tag#micro-}" || return 1
            ;;
        testing/mini)
            tag_pattern='+([0-9]).+([0-9]).+([0-9])/@(aws|gcp)/@(large|xlarge|xxlarge)'
            [[ "${__tag}" = $tag_pattern ]] && echo "${__tag%%/*}" || return 1
            ;;
        1-trackers/golang-tracker)
            tag_pattern='v+([0-9]).+([0-9]).+([0-9])'
            [[ "${__tag}" = $tag_pattern ]] && echo "${__tag#v}" || return 1
            ;;
        5-data-modeling/analytics-sdk/golang)
            tag_pattern='v+([0-9]).+([0-9]).+([0-9])'
            [[ "${__tag}" = $tag_pattern ]] && echo "${__tag#v}" || return 1
            ;;
        5-data-modeling/data-models)
            tag_pattern='**/+([0-9]).+([0-9]).+([0-9])'
            [[ "${__tag}" = $tag_pattern ]] && echo "${__tag}" || return 1
            ;;
        *)
            tag_pattern='+([0-9]).+([0-9]).+([0-9])'
            [[ "${__tag}" = $tag_pattern ]] && echo "${__tag}" || return 1
            ;;
    esac
)

# Propagates a bump merge to the superproject
function merge_bump() {
    [ "$#" -eq 5 ] || die "5 arguments required, $# provided to 'func:merge_n_bump'"
    local __top_level="$1"
    local __mod_name="$2"
    local __rel_path="$3"
    local __bump_commit="$4"
    local __bump_tag="$5"

    # 1. checkout new release commit in submodule
    git checkout "${__bump_commit}"

    # 2. go toplevel
    cd "${__top_level}"

    # 3. get repo name for commit message
    repo_url=`git config -f .gitmodules submodule."${__mod_name}".url`
    [ -z "${repo_url}" ] && die "Submodule url not found for ${__mod_name}"
    repo_name=`basename "${repo_url}" .git`
    commit_msg="${repo_name}: Release ${__bump_tag}"

    # 4. commit and tag in parent
    git add "${__rel_path}"
    git commit -m "${commit_msg}"
    git tag -a "${repo_name}/${__bump_tag}" -m "${repo_name}/${__bump_tag}"

    # 5. go back
    cd "${__rel_path}"
}
