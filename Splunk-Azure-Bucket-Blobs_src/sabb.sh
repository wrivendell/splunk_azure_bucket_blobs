#!/bin/bash

### Python3
python3 sabb.py \
    -do False \
    -lco True \
    -cs "DefaultEndpointsProtocol=https;AccountName=psdfeazuretest;AccountKey=tkjQwSIYEc+quRbWYO+6Bk0QAMJA2Y561KjA98fpi1JT9SLCD9lDjFRMLNhFQZICn5H/t106UTB1BAgStvLomQ==;EndpointSuffix=core.windows.net"               \
    -dl "./blob_downloads/"            \
    -tc "20" \
    -sa True \
    -sph "/opt/splunk/" \
    -spu "admin" \
    -spw "SpluNkP@ssWord" \
    -ll 3 \
    -csl "vmt0pc"\
    -cslt False \
    -bsl "frozendb"\
    -bslt False \
    -dm True \
    -woflo False


# SEE README FOR ARG DETAILS - THIS SH file is for running the compiled AIO(all in one) version
# You should always try and use this version as it doesn't need any outside dependencies
# \  = indicates cmd continues on next line in bash
# do = detailed output (console only, doesn't affect logging)
# lco = list_create_output - gives more feedback during the list creation portion
# cs = Azure Connection String
# dl = destination download root (where the blobs will download to)
# tc = thread count - how many downloads to have active at once
# sa = stand alone - True if running this on a non-clustered environment to get all downloads to one idx, otherwise False and run a copy of this on EACH IDX
# sph = splunk_home - can be left out and "/opt/splunk/" will be used
# spu = splunk_username - required for Cluster run, not for standalone
# spw = splunk_password - required for Cluster run, not for standalone
# cm = cluster_master - helpful if provided, include the https:// - CAN be left out and the script will try and find it. It usually does :)
# cmp = cluster_master_port - API port if not 8089, otherwise dont use this 
# ll = logging_level - 1 less 3 most
# csl = container_search_list, values in quotes separated by commas. "vmt0pc" - only search where value "vmt0pc" is exact or contained in the container names
# cslt = container_search_list_type - True means the items in the above list MUST be exact to the container names, False means the container must only contain the value...no pun intended
# bsl = blob_search_list - "frozendb" - same as csl but for BLOB names inside each container
# bslt = blob_search_list_type - False     - same as cslt but for blob name search list
# cigl = container_ignore_list - same as csl but values are EXCLUDED instead of included in the list. Values matched here will be ignored
# ciglt = container_ignore_list_type - same as cslt but for ignore list above
# bigl = blob_ignore_list - same as cigl but for blob names found inside containers
# biglt = blob_ignore_list_type - same as ciglt but for the blob_ignore_list above
# dm = debug_modules - Will enable deep level debug on all the modules that make up the script. Enable if getting errors, to help dev pinpoint
# woflo = write_out_full_list_only - True will write out the entire list for all peers to a single CSV and do nothing else. Should set -dm to False when using this unless you have errors