#!/bin/bash
# Copyright 2014 CloudHarmony Inc.
# 
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# 
#     http://www.apache.org/licenses/LICENSE-2.0
# 
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


if [ "$1" == "-h" ] || [ "$1" == "--help" ] ; then
  cat << EOF
Usage: run.sh [options]

This repository provides an execution wrapper for the open source OLTP 
Benchmark Framework [http://oltpbenchmark.com] (included as a sub-module in
lib/oltpbench).


RUNTIME PARAMETERS
The following runtime parameters and environment metadata may be specified 
(using run.sh arguments):

--collectd_rrd              If set, collectd rrd stats will be captured from 
                            --collectd_rrd_dir. To do so, when testing starts,
                            existing directories in --collectd_rrd_dir will 
                            be renamed to .bak, and upon test completion 
                            any directories not ending in .bak will be zipped
                            and saved along with other test artifacts (as 
                            collectd-rrd.zip). User MUST have sudo privileges
                            to use this option
                            
--collectd_rrd_dir          Location where collectd rrd files are stored - 
                            default is /var/lib/collectd/rrd

--meta_compute_service      The name of the compute service this test pertains
                            to. May also be specified using the environment 
                            variable bm_compute_service
                            
--meta_compute_service_id   The id of the compute service this test pertains
                            to. Added to saved results. May also be specified 
                            using the environment variable bm_compute_service_id
                            
--meta_cpu                  CPU descriptor - if not specified, it will be set 
                            using the 'model name' attribute in /proc/cpuinfo
                            
--meta_instance_id          The compute service instance type this test pertains 
                            to (e.g. c3.xlarge). May also be specified using 
                            the environment variable bm_instance_id
                            
--meta_memory               Memory descriptor - if not specified, the system
                            memory size will be used
                            
--meta_os                   Operating system descriptor - if not specified, 
                            it will be taken from the first line of /etc/issue
                            
--meta_provider             The name of the cloud provider this test pertains
                            to. May also be specified using the environment 
                            variable bm_provider
                            
--meta_provider_id          The id of the cloud provider this test pertains
                            to. May also be specified using the environment 
                            variable bm_provider_id
                            
--meta_region               The compute service region this test pertains to. 
                            May also be specified using the environment 
                            variable bm_region
                            
--meta_resource_id          An optional benchmark resource identifiers. May 
                            also be specified using the environment variable 
                            bm_resource_id
                            
--meta_run_id               An optional benchmark run identifiers. May also be 
                            specified using the environment variable bm_run_id
                            
--meta_storage_config       Storage configuration descriptor. May also be 
                            specified using the environment variable 
                            bm_storage_config
                            
--meta_test_id              Identifier for the test. May also be specified 
                            using the environment variable bm_test_id
                            
--output                    The output directory to use for writing test data 
                            (logs and artifacts). If not specified, the current 
                            working directory will be used
                            
--verbose                   Show verbose output
                            
                            
DEPENDENCIES
This benchmark has the following dependencies:

java        Java JDK

php         Test automation scripts (/usr/bin/php)

zip         Used to compress test artifacts


USAGE
# run 1 test iteration with some metadata
./run.sh --meta_compute_service_id aws:ec2 --meta_instance_id c3.xlarge --meta_region us-east-1 --meta_test_id aws-0315


EXIT CODES:
  0 test successful
  1 test failed

EOF
  exit
elif [ -f "/usr/bin/php" ]; then
  $( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )/lib/run.php $@
  exit $?
else
  echo "Error: missing dependency php-cli (/usr/bin/php)"
  exit 1
fi
