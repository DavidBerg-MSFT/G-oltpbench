#!/usr/bin/php -q
<?php
// Copyright 2014 CloudHarmony Inc.
// 
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
// 
//     http://www.apache.org/licenses/LICENSE-2.0
// 
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.


/**
 * saves results based on the arguments defined in ../run.sh
 */
require_once(dirname(__FILE__) . '/OltpBenchTest.php');
require_once(dirname(__FILE__) . '/benchmark/save/BenchmarkDb.php');
$status = 1;
$args = parse_args(array('iteration:', 'nostore_html', 'nostore_pdf', 'nostore_rrd', 'nostore_zip', 'v' => 'verbose'));

// get result directories => each directory stores 1 iteration of results
$dirs = array();
$dir = count($argv) > 1 && is_dir($argv[count($argv) - 1]) ? $argv[count($argv) - 1] : trim(shell_exec('pwd'));
if (is_dir(sprintf('%s/1', $dir))) {
  $i = 1;
  while(is_dir($sdir = sprintf('%s/%d', $dir, $i++))) $dirs[] = $sdir;
}
else $dirs[] = $dir;

if ($db =& BenchmarkDb::getDb()) {
  // get results from each directory
  foreach($dirs as $i => $dir) {
    $test = new OltpBenchTest($dir);
    $iteration = isset($args['iteration']) && preg_match('/([0-9]+)/', $args['iteration'], $m) ? $m[1]*1 : $i + 1;
    if ($results = $test->getResults()) {
      print_msg(sprintf('Saving results in directory %s with %d rows', $dir, count($results)), isset($args['verbose']), __FILE__, __LINE__);
      foreach(array('nostore_html' => 'report.zip', 'nostore_pdf' => 'report.pdf', 'nostore_zip' => 'oltp-bench.zip') as $arg => $file) {
        $file = sprintf('%s/%s', $dir, $file);
        if (!isset($args[$arg]) && file_exists($file)) {
          $col = $arg == 'nostore_html' ? 'report_zip' : ($arg == 'nostore_pdf' ? 'report_pdf' : ($arg == 'nostore_rrd' ? 'collectd_rrd' : 'oltp_files'));
          $saved = $db->saveArtifact($file, $col);
          if ($saved) print_msg(sprintf('Saved %s successfully', basename($file)), isset($args['verbose']), __FILE__, __LINE__);
          else if ($saved === NULL) print_msg(sprintf('Unable to save %s', basename($file)), isset($args['verbose']), __FILE__, __LINE__, TRUE);
          else print_msg(sprintf('Artifact %s will not be saved because --store was not specified', basename($file)), isset($args['verbose']), __FILE__, __LINE__);
        }
        else if (file_exists($file)) print_msg(sprintf('Artifact %s will not be saved because --%s was set', basename($file), $arg), isset($args['verbose']), __FILE__, __LINE__);
      }
      $saved = array();
      foreach($results as $n => $row) {
        $test = $row['test'];
        $row['iteration'] = $iteration;
        // save collectd rrd files
        if (!isset($args['nostore_rrd']) && !isset($saved[$test]) && file_exists($file = sprintf('%s/collectd-rrd-%s.zip', $dir, $test))) {
          $saved[$test] = $db->saveArtifact($file, 'collectd_rrd');
          if ($saved[$test]) print_msg(sprintf('Saved collectd rrd files %s successfully', basename($file)), isset($args['verbose']), __FILE__, __LINE__);
          else if ($saved[$test] === NULL) print_msg(sprintf('Unable to save collectd rrd files %s', basename($file)), isset($args['verbose']), __FILE__, __LINE__, TRUE);
        }
        if ($db->addRow($test, $row)) print_msg(sprintf('Successfully saved test results row %d', $n+1), isset($args['verbose']), __FILE__, __LINE__);
        else print_msg(sprintf('Failed to save test results'), isset($args['verbose']), __FILE__, __LINE__, TRUE); 
      }
    }
    else print_msg(sprintf('Unable to save results in directory %s - are result files present?', $dir), isset($args['verbose']), __FILE__, __LINE__, TRUE);
  }
  
  // finalize saving of results
  if ($db->save()) {
    print_msg(sprintf('Successfully saved test results from directory %s', $dir), isset($args['verbose']), __FILE__, __LINE__);
    $status = 0;
  }
  else {
    print_msg(sprintf('Unable to save test results from directory %s', $dir), isset($args['verbose']), __FILE__, __LINE__, TRUE);
    $status = 1;
  }
}

exit($status);
?>
