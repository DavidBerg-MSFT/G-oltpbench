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
 * Used to manage OLTP Benchmark testing
 */
require_once(dirname(__FILE__) . '/benchmark/util.php');
ini_set('memory_limit', '256m');
date_default_timezone_set('UTC');

class OltpBenchTest {
  
  /**
   * date format string
   */
  const OLTP_BENCH_DATE_FORMAT = 'm/d/Y H:i e';
  
  /**
   * database date format
   */
  const OLTP_BENCH_DB_DATA_FORMAT = 'Y-m-d H:i:s';
  
  /**
   * name of the file where serializes options should be written to for given 
   * test iteration
   */
  const OLTP_BENCH_TEST_OPTIONS_FILE_NAME = '.options';
  
  /**
   * optional results directory object was instantiated for
   */
  private $dir;
  
  /**
   * graph colors array
   */
  private $graphColors = array();
  
  /**
   * TRUE if not explicit test rate was set
   */
  private $noRate = FALSE;
  
  /**
   * run options
   */
  private $options;
  
  /**
   * max number of clients used
   */
  private $maxClients;
  
  /**
   * TRUE if database is MySQL or PostgreSQL
   */
  private $mysqlOrPostgres;
  
  /**
   * array containing test results for each test/subtest/step. The key in this
   * array is [test]-[subtest]-[step #] and the value is a hash with the 
   * following keys:
   *   test => test performed
   *   jpab_test => JPAB subtest (jpab only)
   *   step => step #
   *   processes => # of processes
   *   test_clients => # of clients per process
   *   rate => rate (if rate limited)
   *   warmup => true if this was the warmup (firs step if --test_warmup set)
   *   step_started => start time (timestamp)
   *   step_stopped => stop time (timestamp)
   *   size => dataset size (MB)
   *   test_size_label => label for test size
   *   test_size_value => test size value
   *   test_time => test time (secs)
   *   db_name => database name
   *   db_load_from_dump => database loaded from dump?
   *   db_load_time => database load time - secs
   *   latency: mean latency - ms
   *   latency_10: 10th percentile latency - ms
   *   latency_20: 20th percentile latency - ms
   *   latency_30: 30th percentile latency - ms
   *   latency_40: 40th percentile latency - ms
   *   latency_50: 50th percentile latency (median) - ms
   *   latency_60: 60th percentile latency - ms
   *   latency_70: 70th percentile latency - ms
   *   latency_80: 80th percentile latency - ms
   *   latency_90: 90th percentile latency - ms
   *   latency_95: 95th percentile latency - ms
   *   latency_99: 99th percentile latency - ms
   *   latency_max_10: 10th percentile latency_max - ms
   *   latency_max_20: 20th percentile latency_max - ms
   *   latency_max_30: 30th percentile latency_max - ms
   *   latency_max_40: 40th percentile latency_max - ms
   *   latency_max_50: 50th percentile latency_max (median) - ms
   *   latency_max_60: 60th percentile latency_max - ms
   *   latency_max_70: 70th percentile latency_max - ms
   *   latency_max_80: 80th percentile latency_max - ms
   *   latency_max_90: 90th percentile latency_max - ms
   *   latency_max_95: 95th percentile latency_max - ms
   *   latency_max_99: 99th percentile latency_max - ms
   *   latency_at_max: latency at max throughput - ms
   *   latency_max: max latency - ms
   *   latency_stdev: throughput standard deviation
   *   latency_values: array of all mean latency values indexed by seconds
   *   latency_values_max: array of all max percentile latency values indexed by seconds
   *   latency_values_min: array of all min latency values indexed by seconds
   *   latency_values_25: array of all 25th percentile latency values indexed by seconds
   *   latency_values_50: array of all 50th (median) percentile latency values indexed by seconds
   *   latency_values_75: array of all 75th percentile latency values indexed by seconds
   *   latency_values_90: array of all 90th percentile latency values indexed by seconds
   *   latency_values_95: array of all 95th percentile latency values indexed by seconds
   *   latency_values_99: array of all 99th percentile latency values indexed by seconds
   *   steady_state: duration (seconds) before steady state achieve (less than 
   *     10% throughput variation in 5 sequential measurements) - null if not 
   *     achieved
   *   throughput: mean throughput - req/sec
   *   throughput_10: 10th percentile throughput - req/sec
   *   throughput_20: 20th percentile throughput - req/sec
   *   throughput_30: 30th percentile throughput - req/sec
   *   throughput_40: 40th percentile throughput - req/sec
   *   throughput_50: 50th percentile throughput (median) - req/sec
   *   throughput_60: 60th percentile throughput - req/sec
   *   throughput_70: 70th percentile throughput - req/sec
   *   throughput_80: 80th percentile throughput - req/sec
   *   throughput_90: 90th percentile throughput - req/sec
   *   throughput_95: 95th percentile throughput - req/sec
   *   throughput_99: 99th percentile throughput - req/sec
   *   throughput_max: max throughput - req/sec
   *   throughput_min: min throughput - req/sec
   *   throughput_stdev: throughput standard deviation
   *   throughput_values: array of all throughput values indexed by seconds
   */
  private $results = array();
  
  /**
   * steps to perform during test - array of hashes with these keys:
   *   clients => number of clients
   *   rate => rate
   *   start => start time for this step (seconds)
   *   stop => stop time for this step (seconds)
   *   time => time (seconds)
   */
  private $steps = array();
  
  /**
   * some tests have sub-tests (e.g. JPAB)
   */
  private $subtest;
  
  /**
   * the current test
   */
  private $test;
  
  /**
   * enable verbose output?
   */
  private $verbose;
  
  
  /**
   * constructor
   * @param string $dir optional results directory object is being instantiated
   * for. If set, runtime parameters will be pulled from the .options file. Do
   * not set when running a test
   */
  public function OltpBenchTest($dir=NULL) {
    $this->dir = $dir;
  }
  
  /**
   * adjusts a value to the best matching log scale for use on a graph
   * @param float $val the value to adjust
   * @param boolean $min adjust to minimum value?
   * @return float
   */
  private static function adjustLogScale($val, $min=FALSE) {
    $adjusted = NULL;
    if (is_numeric($val) && $val >= 0) {
      $adjusted = 1;
      if ($min) {
        while($val > $adjusted) $adjusted *= 10;
        if ($adjusted > 1) $adjusted /= 10;
      }
      else {
        while($val < $adjusted) $adjusted *= 10;
      }
    }
    return $adjusted;
  }
  
  /**
   * builds the OLTP-Bench XML configuration file. File is written to 
   * [output]/[benchmark].xml and the file path is returned
   * @return string
   */
  private function buildOltpBenchConfig() {
    $dbUrl = str_replace('[benchmark]', $this->test, $this->options['db_url']);
    $fp = fopen($config = sprintf('%s/%s.xml', $this->options['output'], $this->test . ($this->test != $this->subtest ? '-' . $this->subtest : '')), 'w');
    print_msg(sprintf('Generating OLTP-Bench XML config %s', $config), $this->verbose, __FILE__, __LINE__);
    fwrite($fp, '<?xml version="1.0"?>');
    fwrite($fp, "\n<parameters>");
    fwrite($fp, sprintf("\n  <dbtype>%s</dbtype>", $this->options['db_type']));
    fwrite($fp, sprintf("\n  <driver>%s</driver>", $this->options['db_driver']));
    fwrite($fp, sprintf("\n  <DBUrl>%s</DBUrl>", $dbUrl));
    fwrite($fp, sprintf("\n  <username>%s</username>", $this->options['db_user']));
    fwrite($fp, sprintf("\n  <password>%s</password>", isset($this->options['db_pswd']) ? $this->options['db_pswd'] : ''));
    fwrite($fp, sprintf("\n  <isolation>%s</isolation>", OltpBenchTest::translateDbIsolation($this->options['db_isolation'])));
    if ($sf = $this->getScaleFactor()) fwrite($fp, sprintf("\n  <scalefactor>%d</scalefactor>", $sf));
    // JPAB test => set sub-test class
    if ($this->test == 'jpab') {
      $pfile = sprintf('%s/jpab-%s-persistence.xml', $this->options['output'], $this->subtest);
      if (!file_exists($pfile)) {
        $persistence = file_get_contents(dirname(__FILE__) . '/persistence-template.xml');
        $persistence = str_replace('[test]', $this->subtest, $persistence);
        $persistence = str_replace('[db_driver]', $this->options['db_driver'], $persistence);
        $persistence = str_replace('[db_url]', $dbUrl, $persistence);
        $persistence = str_replace('[db_user]', $this->options['db_user'], $persistence);
        $persistence = str_replace('[db_pswd]', isset($this->options['db_pswd']) ? $this->options['db_pswd'] : '', $persistence);
        $persistence = str_replace('[db_dialect]', $this->getHibernateDialect(), $persistence);
        if (file_put_contents($pfile, $persistence)) print_msg(sprintf('Generated JPA persistence.xml file in %s', $pfile), $this->verbose, __FILE__, __LINE__);
        else print_msg(sprintf('Unable to write JPA persistence.xml file %s', $pfile), $this->verbose, __FILE__, __LINE__, TRUE);
      }
      if (file_exists($pfile)) {
        $oltpPfile = dirname(__FILE__) . '/oltpbench/build/META-INF/persistence.xml';
        exec(sprintf('rm -f %s', $oltpPfile));
        exec(sprintf('cp %s %s', $pfile, $oltpPfile));
        print_msg(sprintf('Copied JPA persistence config from %s to %s', basename($pfile), $oltpPfile), $this->verbose, __FILE__, __LINE__); 
      }
      // unable to write persistence file - testing cannot proceed
      else {
        fclose($fp);
        return NULL;
      }
      fwrite($fp, sprintf("\n  <persistence-unit>Hibernate-%s</persistence-unit>", $this->subtest));
      fwrite($fp, sprintf("\n  <testClass>%s</testClass>", OltpBenchTest::getJpabTestClass($this->subtest)));
    }
    // Twitter trace files
    else if ($this->test == 'twitter') {
      fwrite($fp, "\n  <tracefile>config/traces/twitter_tweetids.txt</tracefile>");
      fwrite($fp, "\n  <tracefile2>config/traces/twitter_user_ids.txt</tracefile2>");
    }
    // Wikipedia trace files
    else if ($this->test == 'wikipedia') {
      fwrite($fp, "\n  <traceOut>10</traceOut>");
      fwrite($fp, "\n  <tracefiledebug>debug_wiki</tracefiledebug>");
      fwrite($fp, "\n  <tracefile>config/traces/longtrace_100k.txt</tracefile>");
    }
    fwrite($fp, sprintf("\n  <terminals>%d</terminals>", $this->maxClients));
    fwrite($fp, "\n  <works>");
    foreach($this->steps as $step) {
      fwrite($fp, "\n    <work>");
      if ($step['clients'] != $this->maxClients) fwrite($fp, sprintf("\n      <active_terminals>%d</active_terminals>", $step['clients']));
      fwrite($fp, sprintf("\n      <time>%d</time>", $step['time']));
      fwrite($fp, sprintf("\n      <rate>%d</rate>", $step['rate']));
      fwrite($fp, sprintf("\n      <weights>%s</weights>", implode(',', $this->getTestWeights())));
      fwrite($fp, "\n    </work>");
    }
    fwrite($fp, "\n  </works>");
    fwrite($fp, "\n  <transactiontypes>");
    foreach($this->getTransactionTypes() as $name => $id) {
      fwrite($fp, "\n    <transactiontype>");
      fwrite($fp, sprintf("\n      <name>%s</name>", $name));
      if ($id) fwrite($fp, sprintf("\n      <id>%s</id>", $id));
      fwrite($fp, "\n    </transactiontype>");
    }
    fwrite($fp, "\n  </transactiontypes>");
    fwrite($fp, "\n</parameters>\n");
    fclose($fp);
    return $config;
  }
  
  /**
   * writes test results and finalizes testing
   * @param int $success number of successful tests performed
   * @return boolean
   */
  private function endTest($success) {
    $ended = FALSE;
    $dir = $this->options['output'];
    
    // add test stop time
    $this->options['test_stopped'] = date(OltpBenchTest::OLTP_BENCH_DB_DATA_FORMAT);
    $this->options['maxClients'] = $this->maxClients;
    $this->options['noRate'] = $this->noRate;
    if ($success) $this->options['results'] = $this->results;
    $this->options['steps'] = $this->steps;
    
    if ($java = get_java_version()) {
      $this->options['java_version'] = sprintf('%s%s', $java['vendor'] ? $java['vendor'] . ' ' : '', $java['version']);
      print_msg(sprintf('Set java_version=%s', $this->options['java_version']), $this->verbose, __FILE__, __LINE__); 
    }
    
    // serialize options
    $ofile = sprintf('%s/%s', $dir, self::OLTP_BENCH_TEST_OPTIONS_FILE_NAME);
    if (is_dir($dir) && is_writable($dir)) {
      $fp = fopen($ofile, 'w');
      fwrite($fp, serialize($this->options));
      fclose($fp);
      $ended = TRUE;
    }
    
    // remove raw output files (very large)
    exec(sprintf('rm -f %s/*.raw', $dir));
    
    // generate results archive oltp-bench.zip
    $zip = sprintf('%s/oltp-bench.zip', $dir);
    exec('rm -f ' . $zip);
    mkdir($tdir = sprintf('%s/%d', $dir, rand()));
    exec(sprintf('cp %s/*.cnf %s/', $dir, $tdir));
    exec(sprintf('cp %s/*.err %s/', $dir, $tdir));
    exec(sprintf('cp %s/*.out %s/', $dir, $tdir));
    exec(sprintf('cp %s/*.res %s/', $dir, $tdir));
    exec(sprintf('cp %s/*.xml %s/', $dir, $tdir));
    exec(sprintf('cp %s/*.sh %s/', $dir, $tdir));
    exec(sprintf('cd %s; zip %s *; mv %s %s; rm -rf %s', $tdir, basename($zip), basename($zip), $dir, $tdir));
    if (file_exists($zip) && !filesize($zip)) exec('rm -f ' . $zip);
    if (file_exists($zip)) print_msg(sprintf('Successfully generated results archive %s', $zip), $this->verbose, __FILE__, __LINE__);
    else print_msg(sprintf('Unable to generate results archive %s', $zip), $this->verbose, __FILE__, __LINE__, TRUE);
    
    // generate test report
    if ($success && $this->results && !isset($this->options['noreport'])) {
      mkdir($tdir = sprintf('%s/%d', $dir, rand()));
      print_msg(sprintf('Generating HTML report in %s', $tdir), $this->verbose, __FILE__, __LINE__);
      if ($this->generateReport($tdir) && file_exists(sprintf('%s/index.html', $tdir))) {
        if (!isset($this->options['nopdfreport'])) {
          print_msg('Generating PDF report using wkhtmltopdf', $this->verbose, __FILE__, __LINE__);
          $cmd = sprintf('cd %s; wkhtmltopdf -s Letter --footer-left [date] --footer-right [page] --footer-font-name rfont --footer-font-size %d index.html report.pdf >/dev/null 2>&1; echo $?', $tdir, $this->options['font_size']);
          $ecode = trim(exec($cmd));
          if ($ecode > 0) print_msg(sprintf('Failed to generate PDF report'), $this->verbose, __FILE__, __LINE__, TRUE);
          else {
            print_msg(sprintf('Successfully generated PDF report'), $this->verbose, __FILE__, __LINE__);
            exec(sprintf('mv %s/report.pdf %s', $tdir, $dir));
          }
        }
        $zip = sprintf('%s/report.zip', $dir);
        exec('rm -f ' . $zip);
        exec(sprintf('cd %s; zip %s *; mv %s %s', $tdir, basename($zip), basename($zip), $dir));
      }
      else print_msg('Unable to generate HTML report', $this->verbose, __FILE__, __LINE__, TRUE);
      
      exec(sprintf('rm -rf %s', $tdir));
    }
    else print_msg('Test report will not be generated because --noreport flag was set or not results exist', $this->verbose, __FILE__, __LINE__);
    
    return $ended;
  }
  
  /**
   * evaluates a string containing an expression. The substring [cpus] will be 
   * replaced with the number of CPU cores
   * @param string $expr the expression to evaluate
   * @return float
   */
  private function evaluateExpression($expr) {
    $sysInfo = get_sys_info();
    $expr = str_replace('[cpus]', isset($sysInfo['cpu_cores']) ? $sysInfo['cpu_cores'] : 2, $expr);
    eval(sprintf('$value=round(%s);', $expr));
    $value *= 1;
    return $value;
  }
  
  /**
   * generates graphs for $result in the directory $dir using the prefix 
   * $prefix
   * @param array $result the results data to generate graphs for
   * @param string $dir directory where graphs should be generate in
   * @param string prefix for generated files
   * @return boolean
   */
  private function generateGraphs($result, $dir, $prefix) {
    $graphs = array();
    if ($result && is_dir($dir) && is_writable($dir) && $prefix) {
      if (isset($result['throughput_values'])) {
        // Throughput Timeline
        $timeline = $this->makeCoords($result['throughput_values']);
        $coords = array('' => $timeline,
                        'Median' => array(array($timeline[0][0], $result['throughput_50']), array($timeline[count($timeline) - 1][0], $result['throughput_50'])));
        $settings = array();
        $settings['lines'] = array(1 => "lt 1 lc rgb '#5DA5DA' lw 3 pt -1",
                                   2 => "lt 2 lc rgb '#4D4D4D' lw 3 pt -1");
        $settings['nogrid'] = TRUE;
        $settings['yMin'] = 0;
        if ($graph = $this->generateGraph($dir, $prefix . '-throughput', $coords, 'Time (secs)', 'Throughput (req/sec)', NULL, $settings)) $graphs[sprintf('Throughput - %d clients', $result['processes']*$result['test_clients'])] = $graph;
        
        // Throughput Histogram
        $coords = $this->makeCoords($result['throughput_values'], TRUE);
        $settings = array();
        $settings['nogrid'] = TRUE;
        $settings['yMin'] = 0;
        $settings['yMax'] = '20%';
        if ($graph = $this->generateGraph($dir, $prefix . '-throughput-histogram', $coords, 'Throughput (req/sec)', 'Samples', NULL, $settings, TRUE, 'histogram')) $graphs[sprintf('Throughput Histogram - %d clients', $result['processes']*$result['test_clients'])] = $graph;
        
        // Throughput Percentiles
        $coords = array();
        foreach(array(10, 20, 30, 40, 50, 60, 70, 80, 90, 95, 99) as $perc) {
          $coords[sprintf('%dth', $perc)] = array($result[sprintf('throughput_%d', $perc)]);
        }
        $settings = array();
        $settings['nogrid'] = TRUE;
        $settings['yMin'] = 0;
        $settings['yMax'] = '20%';
        if ($graph = $this->generateGraph($dir, $prefix . '-throughput-percentiles', $coords, 'Percentiles', 'Throughput (req/sec)', NULL, $settings, TRUE, 'bar')) $graphs[sprintf('Throughput Percentiles - %d clients', $result['processes']*$result['test_clients'])] = $graph;
      }
      if (isset($result['latency_values'])) {
        // Latency Timeline
        $coords = array('Median' => $this->makeCoords($result['latency_values_50']),
                        'Min' => $this->makeCoords($result['latency_values_min']),
                        'Max' => $this->makeCoords($result['latency_values_max']));
        $settings['lines'] = array(1 => "lt 2 lc rgb '#4D4D4D' lw 4 pt -1",
                                   2 => "lt 1 lc rgb '#60BD68' lw 3 pt -1",
                                   3 => "lt 1 lc rgb '#F15854' lw 3 pt -1");
        $settings['nogrid'] = TRUE;
        $settings['yMin'] = 0;
        if ($graph = $this->generateGraph($dir, $prefix . '-latency', $coords, 'Time (secs)', 'Latency (ms)', NULL, $settings)) $graphs[sprintf('Latency - %d clients', $result['processes']*$result['test_clients'])] = $graph;

        // Latency Histogram
        $coords = $this->makeCoords($result['latency_values_max'], TRUE);
        $settings = array();
        $settings['nogrid'] = TRUE;
        $settings['yMin'] = 0;
        $settings['yMax'] = '20%';
        if ($graph = $this->generateGraph($dir, $prefix . '-latency-histogram', $coords, 'Max Latency (ms)', 'Samples', NULL, $settings, TRUE, 'histogram')) $graphs[sprintf('Max Latency Histogram - %d clients', $result['processes']*$result['test_clients'])] = $graph;
        
        // Latency Percentiles
        $coords = array();
        foreach(array(10, 20, 30, 40, 50, 60, 70, 80, 90, 95, 99) as $perc) {
          $coords[sprintf('%dth', $perc)] = array($result[sprintf('latency_max_%d', $perc)]);
        }
        $settings = array();
        $settings['nogrid'] = TRUE;
        $settings['yMin'] = 0;
        $settings['yMax'] = '20%';
        if ($graph = $this->generateGraph($dir, $prefix . '-latency-percentiles', $coords, 'Percentiles', 'Max Latency (ms)', NULL, $settings, TRUE, 'bar')) $graphs[sprintf('Max Latency Percentiles - %d clients', $result['processes']*$result['test_clients'])] = $graph;
      }
    }
    return $graphs;
  }
  
  /**
   * generates a line chart based on the parameters provided. return value is 
   * the name of the image which may in turn be used in an image element for 
   * a content section. returns NULL on error
   * @param string $dir the directory where the line chart should be generated
   * @param string $prefix the file name prefix
   * @param array $coords either a single array of tuples representing the x/y
   * values, or a hash or tuple arrays indexed by the name of each set of data
   * points. coordinates should have the same index
   * @param string $xlabel optional x label
   * @param string $ylabel optional y label
   * @param string $title optional graph title
   * @param array $settings optional array of custom gnuplot settings. the 
   * following special settings are supported:
   *   height: the graph height
   *   lines:     optional line styles (indexed by line #)
   *   nogrid:    don't add y axis grid lines
   *   nokey:     don't show the plot key/legend
   *   nolinespoints: don't use linespoints
   *   xFloatPrec: x float precision
   *   xLogscale: use logscale for the x axis
   *   xMin:      min value for the x axis tics - may be a percentage relative to 
   *              the lowest value
   *   xMax:      max value for the x axis tics - may be a percentage relative to 
   *              the highest value
   *   xTics:     the number of x tics to show (default 8)
   *   yFloatPrec: y float precision
   *   yLogscale: use logscale for the y axis
   *   yMin:      min value for the x axis tics - may be a percentage relative to 
   *              the lowest value
   *   yMax:      max value for the y axis tics - may be a percentage relative to 
   *              the highest value
   *   yTics:     the number of y tics to show (default 8)
   * 
   * xMin, xMax, yMin and yMax all default to the same value as the other for 
   * percentages and 15% otherwise if only 1 is set for a given 
   * axis. If neither are specified, gnuplot will auto assign the tics. If xMin
   * or xMax are specified, but not xTics, xTics defaults to 8
   * @param boolean $html whether or not to return the html <img element or just
   * the name of the file
   * @param string $type the type of graph to generate - line, histogram or bar. 
   * If histogram, $coords should represent all of the y values for a 
   * given X. The $coords hash key will be used as the X label and the value(s) 
   * rendered using a clustered histogram (grouped column chart)
   * @return string
   */
  private function generateGraph($dir, $prefix, $coords, $xlabel=NULL, $ylabel=NULL, $title=NULL, $settings=NULL, $html=TRUE, $type='line') {
    print_msg(sprintf('Generating line chart in %s using prefix %s with %d coords', $dir, $prefix, count($coords)), $this->verbose, __FILE__, __LINE__);
    
    $chart = NULL;
    $script = sprintf('%s/%s.pg', $dir, $prefix);
    $dfile = sprintf('%s/%s.dat', $dir, $prefix);
    if (is_array($coords) && ($fp = fopen($script, 'w')) && ($df = fopen($dfile, 'w'))) {
      $colors = $this->getGraphColors();
      $xFloatPrec = isset($settings['xFloatPrec']) && is_numeric($settings['xFloatPrec']) ? $settings['xFloatPrec'] : 0;
      $yFloatPrec = isset($settings['yFloatPrec']) && is_numeric($settings['yFloatPrec']) ? $settings['yFloatPrec'] : 0;
      
      // just one array of tuples
      if (isset($coords[0])) $coords = array('' => $coords);
      
      // determine max points/write data file header
      $maxPoints = NULL;
      foreach(array_keys($coords) as $i => $key) {
        if ($maxPoints === NULL || count($coords[$key]) > $maxPoints) $maxPoints = count($coords[$key]);
        if ($type == 'line') fwrite($df, sprintf("%s%s%s\t%s%s", $i > 0 ? "\t" : '', $key ? $key . ' ' : '', $xlabel ? $xlabel : 'X', $key ? $key . ' ' : '', $ylabel ? $ylabel : 'Y'));
      }
      if ($type == 'line') fwrite($df, "\n");
      
      // determine value ranges and generate data file
      $minX = NULL;
      $maxX = NULL;
      $minY = NULL;
      $maxY = NULL;
      if ($type != 'line') {
        foreach($coords as $x => $points) {
          if ($type == 'bar' && is_numeric($x) && ($minX === NULL || $x < $minX)) $minX = $x;
          if ($type == 'bar' && is_numeric($x) && $x > $maxX) $maxX = $x;
          fwrite($df, $x);
          for($n=0; $n<$maxPoints; $n++) {
            $y = isset($points[$n]) && is_numeric($points[$n]) ? $points[$n]*1 : '';
            if (is_numeric($y) && ($minY === NULL || $y < $minY)) $minY = $y;
            if (is_numeric($y) && $y > $maxY) $maxY = $y;
            fwrite($df, sprintf("\t%s", $y));
          }
          fwrite($df, "\n");
        }
        if ($type == 'histogram') fwrite($df, ".\t0\n");
      }
      else {
        for($n=0; $n<$maxPoints; $n++) {
          foreach(array_keys($coords) as $i => $key) {
            $x = isset($coords[$key][$n][0]) ? $coords[$key][$n][0] : '';
            if (is_numeric($x) && ($minX === NULL || $x < $minX)) $minX = $x;
            if (is_numeric($x) && $x > $maxX) $maxX = $x;
            $y = isset($coords[$key][$n][1]) ? $coords[$key][$n][1] : '';
            if (is_numeric($y) && ($minY === NULL || $y < $minY)) $minY = $y;
            if (is_numeric($y) && $y > $maxY) $maxY = $y;
            fwrite($df, sprintf("%s%s\t%s", $i > 0 ? "\t" : '', $x, $y));
          }
          fwrite($df, "\n");
        } 
      }
      fclose($df);
      
      // determine x tic settings
      $xMin = isset($settings['xMin']) ? $settings['xMin'] : NULL;
      $xMax = isset($settings['xMax']) ? $settings['xMax'] : NULL;
      $xTics = isset($settings['xTics']) ? $settings['xTics'] : NULL;
      if (!isset($xMin) && (isset($xMax) || $xTics)) $xMin = isset($xMax) && preg_match('/%/', $xMax) ? $xMax : '15%';
      if (!isset($xMax) && (isset($xMin) || $xTics)) $xMax = isset($xMin) && preg_match('/%/', $xMin) ? $xMin : '15%';
      if (!isset($xMin)) $xMin = $minX;
      if (!isset($xMax)) $xMax = $maxX;
      if (preg_match('/^([0-9\.]+)%$/', $xMin, $m)) {
        $xMin = floor($minX - ($minX*($m[1]*0.01)));
        if ($xMin < 0) $xMin = 0;
      }
      if (preg_match('/^([0-9\.]+)%$/', $xMax, $m)) $xMax = ceil($maxX + ($maxX*($m[1]*0.01)));
      if (!$xTics) $xTics = 8;
      $xDiff = $xMax - $xMin;
      $xStep = floor($xDiff/$xTics);
      if ($xStep < 1) $xStep = 1;
      
      // determine y tic settings
      $yMin = isset($settings['yMin']) ? $settings['yMin'] : NULL;
      $yMax = isset($settings['yMax']) ? $settings['yMax'] : NULL;
      $yTics = isset($settings['yTics']) ? $settings['yTics'] : NULL;
      if (!isset($yMin) && (isset($yMax) || $yTics)) $yMin = isset($yMax) && preg_match('/%/', $yMax) ? $yMax : '15%';
      if (!isset($yMax) && (isset($yMin) || $yTics)) $yMax = isset($yMin) && preg_match('/%/', $yMin) ? $yMin : '15%';
      if (isset($yMin) && preg_match('/^([0-9\.]+)%$/', $yMin, $m)) {
        $yMin = floor($minY - ($minY*($m[1]*0.01)));
        if ($yMin < 0) $yMin = 0;
      }
      if (isset($yMin)) {
        if (preg_match('/^([0-9\.]+)%$/', $yMax, $m)) $yMax = ceil($maxY + ($maxY*($m[1]*0.01)));
        if (!$yTics) $yTics = 8;
        $yDiff = $yMax - $yMin;
        $yStep = floor($yDiff/$yTics);
        if ($yStep < 1) $yStep = 1;
      }
      
      $img = sprintf('%s/%s.svg', $dir, $prefix);
      print_msg(sprintf('Generating line chart %s with %d data sets and %d points/set. X Label: %s; Y Label: %s; Title: %s', basename($img), count($coords), $maxPoints, $xlabel, $ylabel, $title), $this->verbose, __FILE__, __LINE__);
      
      fwrite($fp, sprintf("#!%s\n", trim(shell_exec('which gnuplot'))));
      fwrite($fp, "reset\n");
      fwrite($fp, sprintf("set terminal svg dashed size 1024,%d fontfile 'font-svg.css' font 'rfont,%d'\n", isset($settings['height']) ? $settings['height'] : 600, $this->options['font_size']+4));
      // custom settings
      if (is_array($settings)) {
        foreach($settings as $key => $setting) {
          // special settings
          if (in_array($key, array('height', 'lines', 'nogrid', 'nokey', 'nolinespoints', 'xLogscale', 'xMin', 'xMax', 'xTics', 'xFloatPrec', 'yFloatPrec', 'yLogscale', 'yMin', 'yMax', 'yTics'))) continue;
          fwrite($fp, "${setting}\n");
        }
      }
      fwrite($fp, "set autoscale keepfix\n");
      fwrite($fp, "set decimal locale\n");
      fwrite($fp, "set format y \"%'10.${yFloatPrec}f\"\n");
      fwrite($fp, "set format x \"%'10.${xFloatPrec}f\"\n");
      if ($xlabel) fwrite($fp, sprintf("set xlabel \"%s\"\n", $xlabel));
      if (isset($settings['xLogscale'])) {
        if (!isset($settings['xMin'])) $xMin = OltpBenchTest::adjustLogScale($xMin, TRUE);
        if (!isset($settings['xMax'])) $xMax = OltpBenchTest::adjustLogScale($xMax);
      }
      if ($xMin != $xMax) fwrite($fp, sprintf("set xrange [%d:%d]\n", $xMin, $xMax));
      if (isset($settings['xLogscale'])) fwrite($fp, "set logscale x\n");
      else if ($xMin != $xMax && !$xFloatPrec) fwrite($fp, sprintf("set xtics %d, %d, %d\n", $xMin, $xStep, $xMax));
      if ($ylabel) fwrite($fp, sprintf("set ylabel \"%s\"\n", $ylabel));
      if (isset($yMin)) {
        if (isset($settings['yLogscale'])) {
          if (!isset($settings['yMin'])) $yMin = OltpBenchTest::adjustLogScale($yMin, TRUE);
          if (!isset($settings['yMax'])) $yMax = OltpBenchTest::adjustLogScale($yMax);
        }
        if ($yMin != $yMax) fwrite($fp, sprintf("set yrange [%d:%d]\n", $yMin, $yMax));
        if (isset($settings['yLogscale'])) fwrite($fp, "set logscale y\n");
        else if (!$yFloatPrec) fwrite($fp, sprintf("set ytics %d, %d, %d\n", $yMin, $yStep, $yMax));
      }
      if ($title) fwrite($fp, sprintf("set title \"%s\"\n", $title));
      if (!isset($settings['nokey'])) fwrite($fp, "set key outside center top horizontal reverse\n");
      fwrite($fp, "set grid\n");
      fwrite($fp, sprintf("set style data lines%s\n", !isset($settings['nolinespoints']) || !$settings['nolinespoints'] ? 'points' : ''));
      
      # line styles
      fwrite($fp, "set border linewidth 1.5\n");
      foreach(array_keys($coords) as $i => $key) {
        if (!isset($colors[$i])) break;
        if (isset($settings['lines'][$i+1])) fwrite($fp, sprintf("set style line %d %s\n", $i+1, $settings['lines'][$i+1]));
        else fwrite($fp, sprintf("set style line %d lc rgb '%s' lt 1 lw 3 pt -1\n", $i+1, $colors[$i]));
      }
      if ($type != 'line') {
        fwrite($fp, "set style fill solid noborder\n");
        fwrite($fp, sprintf("set boxwidth %s relative\n", $type == 'histogram' ? '1' : '0.9'));
        fwrite($fp, sprintf("set style histogram cluster gap %d\n", $type == 'histogram' ? 0 : 1));
        fwrite($fp, "set style data histogram\n");
      }
      
      fwrite($fp, "set grid noxtics\n");
      if (!isset($settings['nogrid'])) fwrite($fp, "set grid ytics lc rgb '#dddddd' lw 1 lt 0\n");
      else fwrite($fp, "set grid noytics\n");
      fwrite($fp, "set tic scale 0\n");
      // centering of labels doesn't work with current CentOS gnuplot package, so simulate for histogram
      if ($type == 'histogram') fwrite($fp, sprintf("set xtics offset %d\n", round(0.08*(480/count($coords)))));
      fwrite($fp, sprintf("plot \"%s\"", basename($dfile)));
      $colorPtr = 1;
      if ($type != 'line') {
        for($i=0; $i<$maxPoints; $i++) {
          fwrite($fp, sprintf("%s u %d:xtic(1) ls %d notitle", $i > 0 ? ", \\\n\"\"" : '', $i+2, $colorPtr));
          $colorPtr++;
          if ($colorPtr > count($colors)) $colorPtr = 1;
        }
      }
      else {
        foreach(array_keys($coords) as $i => $key) {
          fwrite($fp, sprintf("%s every ::1 u %d:%d t \"%s\" ls %d", $i > 0 ? ", \\\n\"\"" : '', ($i*2)+1, ($i*2)+2, $key, $colorPtr));
          $colorPtr++;
          if ($colorPtr > count($colors)) $colorPtr = 1;
        }
      }
      
      fclose($fp);
      exec(sprintf('chmod +x %s', $script));
      $cmd = sprintf('cd %s; ./%s > %s 2>/dev/null; echo $?', $dir, basename($script), basename($img));
      $ecode = trim(exec($cmd));
      // exec('rm -f %s', $script);
      // exec('rm -f %s', $dfile);
      if ($ecode > 0) {
        // exec('rm -f %s', $img);
        // passthru(sprintf('cd %s; ./%s > %s', $dir, basename($script), basename($img)));
        // print_r($coords);
        // echo $cmd;
        // exit;
        print_msg(sprintf('Failed to generate line chart - exit code %d', $ecode), $this->verbose, __FILE__, __LINE__, TRUE);
      }
      else {
        print_msg(sprintf('Generated line chart %s successfully', $img), $this->verbose, __FILE__, __LINE__);
        // attempt to convert to PNG using wkhtmltoimage
        if (OltpBenchTest::wkhtmltopdfInstalled()) {
          $cmd = sprintf('wkhtmltoimage %s %s >/dev/null 2>&1', $img, $png = str_replace('.svg', '.png', $img));
          $ecode = trim(exec($cmd));
          if ($ecode > 0 || !file_exists($png) || !filesize($png)) print_msg(sprintf('Unable to convert SVG image %s to PNG %s (exit code %d)', $img, $png, $ecode), $this->verbose, __FILE__, __LINE__, TRUE);
          else {
            exec(sprintf('rm -f %s', $img));
            print_msg(sprintf('SVG image %s converted to PNG successfully - PNG will be used in report', basename($img)), $this->verbose, __FILE__, __LINE__);
            $img = $png;
          }
        }
        // return full image tag
        if ($html) $chart = sprintf('<img alt="" class="plot" src="%s" />', basename($img));
        else $chart = basename($img);
      }
    }
    // error - invalid scripts or unable to open gnuplot files
    else {
      print_msg(sprintf('Failed to generate line chart - either coordinates are invalid or script/data files %s/%s could not be opened', basename($script), basename($dfile)), $this->verbose, __FILE__, __LINE__, TRUE);
      if ($fp) {
        fclose($fp);
        exec('rm -f %s', $script);
      }
    }
    return $chart;
  }
  
  /**
   * generates an HTML report. Returns TRUE on success, FALSE otherwise
   * @param string $dir optional directory where reports should be generated 
   * in. If not specified, --output will be used
   * @return boolean
   */
  public function generateReport($dir=NULL) {
    $generated = FALSE;
    $pageNum = 0;
    if (!$dir) $dir = $this->options['output'];
    
    if (is_dir($dir) && is_writable($dir) && ($fp = fopen($htmlFile = sprintf('%s/index.html', $dir), 'w'))) {
      print_msg(sprintf('Initiating report creation in directory %s', $dir), $this->verbose, __FILE__, __LINE__);
      
      $reportsDir = dirname(dirname(__FILE__)) . '/reports';
      $fontSize = $this->options['font_size'];
      
      // add header
      $tests = array();
      $subtests = array();
      foreach(array_keys($this->results) as $key) {
        $test = $this->results[$key]['test'];
        if (!in_array($test, $tests)) $tests[] = $test;
        if (isset($this->results[$key]['subtest'])) {
          if (!isset($subtests[$test])) $subtests[$test] = array();
          $subtests[$test][] = $this->results[$key]['subtest'];
        }
      }
      $title = 'OLTP Performance Report - ' . implode(' ', $tests);
      
      ob_start();
      include(sprintf('%s/header.html', $reportsDir));
      fwrite($fp, ob_get_contents());
      ob_end_clean();
      
      // copy font files
      exec(sprintf('cp %s/font-svg.css %s/', $reportsDir, $dir));
      exec(sprintf('cp %s/font.css %s/', $reportsDir, $dir));
      exec(sprintf('cp %s/font.ttf %s/', $reportsDir, $dir));
      exec(sprintf('cp %s/logo.png %s/', $reportsDir, $dir));
      
      foreach($tests as $test) {
        foreach(isset($subtests[$test]) ? $subtests[$test] : array($test) as $subtest) {
          $results = array();
          $fkey = NULL;
          $lkey = NULL;
          $resultClients = array();
          $resultRates = array();
          $resultTp = array();
          $resultLatency = array();
          $resultLatencyAtMax = array();
          $resultSs = array();
          foreach(array_keys($this->results) as $key) {
            if ($this->results[$key]['test'] == $test && ($subtest == $test || (isset($this->results[$key]['subtest']) && $this->results[$key]['subtest'] == $subtest))) {
              if (isset($this->results[$key]['warmup']) && $this->results[$key]['warmup']) continue;
              if (!$fkey) $fkey = $key;
              $lkey = $key;
              $results[$key] = $this->results[$key];
              $resultClients[] = $this->results[$key]['test_clients'];
              $resultRates[] = isset($this->results[$key]['test_rate']) ? $this->results[$key]['test_rate'] : 'Unlimited';
              $resultTp[] = $this->results[$key]['throughput'];
              $resultLatency[] = $this->results[$key]['latency_max'];
              $resultLatencyAtMax[] = $this->results[$key]['latency_at_max'];
              if (isset($this->results[$key]['steady_state'])) $resultSs[] = $this->results[$key]['steady_state'];
            }
            if (count(array_unique($resultRates)) == 1 && in_array('Unlimited', $resultRates)) $resultRates = array('Unlimited');
          }
          print_msg(sprintf('Generating content for test %s with %d results', $test, count($results)), $this->verbose, __FILE__, __LINE__);
          $testPageNum = 0;

          // page header
          $dbParams = array('Type' => $this->options['db_type'],
                            'Name' => $results[$lkey]['db_name'],
                            'Driver' => $this->options['db_driver'],
                            'Isolation Level' => $this->options['db_isolation'],
                            'Load Time' => isset($results[$lkey]['db_load_time']) ? $results[$lkey]['db_load_time'] . ' secs' : 'Not Loaded',
                            'Loaded from Dump?' => isset($results[$lkey]['db_load_time']) ? ($results[$lkey]['db_load_from_dump'] ? 'Yes' : 'No') : 'NA',
                            'Dataset Size' => isset($results[$lkey]['size']) ? $results[$lkey]['size'] . ' MB' : 'NA');
          if (isset($results[$lkey]['test_size_label'])) $dbParams[$results[$lkey]['test_size_label']] = $results[$lkey]['test_size_value'];

          $graphs = array();
          $gresults = array();

          // graph by clients
          $testPages = count($results)*6;
          if (count($results) > 1) {
            $testPages += 2;
            $tcoords = array();
            $lcoords = array();
            foreach($results as $result) {
              $tcoords[sprintf('%d clients', $result['processes']*$result['test_clients'])] = $this->makeCoords($result['throughput_values']);
              $lcoords[sprintf('%d clients', $result['processes']*$result['test_clients'])] = $this->makeCoords($result['latency_values_max']);
            }
            $settings = array();
            $settings['nogrid'] = TRUE;
            $settings['yMin'] = 0;
            if ($graph = $this->generateGraph($dir, $test . '-throughput-by-client', $tcoords, 'Time (secs)', 'Throughput (req/sec)', NULL, $settings)) $graphs['Throughput by Clients'] = $graph;
            if ($graph = $this->generateGraph($dir, $test . '-latency-by-client', $lcoords, 'Time (secs)', 'Max Latency (ms)', NULL, $settings)) $graphs['Max Latency by Clients'] = $graph;
          }

          foreach($results as $key => $result) {
            if ($rgraphs = $this->generateGraphs($result, $dir, $key)) {
              $graphs = array_merge($graphs, $rgraphs);
              foreach(array_keys($rgraphs) as $label) $gresults[$label] = $result;
            }
            else print_msg(sprintf('Unable to generate graphs for %s', $key), $this->verbose, __FILE__, __LINE__, TRUE);
          }

          // render report graphs (1 per page)
          foreach($graphs as $label => $graph) {
            $result = isset($gresults[$label]) ? $gresults[$label] : NULL;

            $params = array(
              'platform' => $this->getPlatformParameters(),
              'database' => $dbParams,
              'test' =>     array('OLTP Test' => strtoupper($results[$lkey]['test']) . ($result && isset($result['jpab_test']) ? ' [' . strtoupper($result['jpab_test']) . ']' : ''),
                                  'Step' => $result ? $result['step'] : '1-' . count($results),
                                  'Processes' => $results[$lkey]['processes'],
                                  'Clients/Process' => $result ? $result['test_clients'] : implode(', ', $resultClients),
                                  'Rate' => $result ? (isset($result['test_rate']) ? $result['test_rate'] : 'Unlimited') : implode(', ', $resultRates),
                                  'Time' => $results[$lkey]['test_time'] . ' secs',
                                  'Started' => date(OltpBenchTest::OLTP_BENCH_DATE_FORMAT, $result ? $result['step_started'] : $results[$fkey]['step_started']),
                                  'Ended' => date(OltpBenchTest::OLTP_BENCH_DATE_FORMAT, $result ? $result['step_stopped'] : $results[$lkey]['step_stopped'])),
              'result' =>   array('Mean Throughput' => ($result ? $result['throughput'] : round(get_mean($resultTp))) . ' req/sec',
                                  'Median Throughput' => ($result ? $result['throughput_50'] : round(get_percentile($resultTp))) . ' req/sec',
                                  'Std Dev' => ($result ? $result['throughput_stdev'] : round(get_std_dev($resultTp))),
                                  'Mean Latency' => ($result ? $result['latency'] : round(get_mean($resultLatency))) . ' ms',
                                  'Median Latency' => ($result ? $result['latency_50'] : round(get_percentile($resultLatency))) . ' ms',
                                  'Std Dev ' => ($result ? $result['latency_stdev'] : round(get_std_dev($resultLatency))),
                                  'Latency at Max Throughput' => ($result ? $result['latency_at_max'] : round(get_mean($resultLatencyAtMax))) . ' ms',
                                  'Steady State' => $result ? (isset($result['steady_state']) ? $result['steady_state'] . ' secs' : 'Not Achieved') : ($resultSs ? round(get_mean($resultSs)) : 'Not Achieved'))
            );
            $headers = array();
            for ($i=0; $i<100; $i++) {
              $empty = TRUE;
              $cols = array();
              foreach($params as $type => $vals) {
                if (count($vals) >= ($i + 1)) {
                  $empty = FALSE;
                  $keys = array_keys($vals);
                  $cols[] = array('class' => $type, 'label' => $keys[$i], 'value' => $vals[$keys[$i]]);
                }
                else $cols[] = array('class' => $type, 'label' => '', 'value' => '');
              }
              if (!$empty) $headers[] = $cols;
              else break;
            }

            $pageNum++;
            $testPageNum++;
            print_msg(sprintf('Successfully generated graphs for %s', $key), $this->verbose, __FILE__, __LINE__);
            ob_start();
            include(sprintf('%s/test.html', $reportsDir, $test));
            fwrite($fp, ob_get_contents());
            ob_end_clean(); 
            $generated = TRUE; 
          }   
        }
      }
      
      // add footer
      ob_start();
      include(sprintf('%s/footer.html', $reportsDir));
      fwrite($fp, ob_get_contents());
      ob_end_clean();
      
      fclose($fp);
    }
    else print_msg(sprintf('Unable to generate report in directory %s - it either does not exist or is not writable', $dir), $this->verbose, __FILE__, __LINE__, TRUE);
    
    return $generated;
  }
  
  /**
   * returns an array containing the hex color codes to use for graphs (as 
   * defined in graph-colors.txt)
   * @return array
   */
  protected final function getGraphColors() {
    if (!count($this->graphColors)) {
      foreach(file(dirname(__FILE__) . '/graph-colors.txt') as $line) {
        if (substr($line, 0, 1) != '#' && preg_match('/([a-zA-Z0-9]{6})/', $line, $m)) $this->graphColors[] = '#' . $m[1];
      }
    }
    return $this->graphColors;
  }
  
  /**
   * returns the Hibernate dialect to use based on the datbase type
   * @return string
   */
  private function getHibernateDialect() {
    $dialect = NULL;
    switch($this->options['db_type']) {
      case 'mysql':
        $dialect = 'org.hibernate.dialect.MySQLDialect';
        break;
      case 'db2':
        $dialect = 'org.hibernate.dialect.DB2Dialect';
        break;
      case 'postgres':
        $dialect = 'org.hibernate.dialect.PostgreSQLDialect';
        break;
      case 'oracle':
        $dialect = 'org.hibernate.dialect.Oracle8iDialect';
        break;
      case 'sqlserver':
        $dialect = 'org.hibernate.dialect.SQLServerDialect';
        break;
    }
    return $dialect;
  }
  
  /**
   * returns the Java class to use for the JPAB test identified by $jpabTest
   * @param string $jpabTest the JPAB test to return the class name for
   * @return string
   */
  private static function getJpabTestClass($jpabTest) {
    $jpabClass = NULL;
    switch($jpabTest) {
      case 'basic':
        $jpabClass = 'BasicTest';
        break;
      case 'collection':
        $jpabClass = 'CollectionTest';
        break;
      case 'inheritance':
        $jpabClass = 'ExtTest';
        break;
      case 'indexing':
        $jpabClass = 'IndexTest';
        break;
      case 'graph':
        $jpabClass = 'NodeTest';
        break;
    }
    return $jpabClass;
  }
  
  /**
   * returns the platform parameters for this test. These are displayed in the 
   * Test Platform columns
   * @return array
   */
  private function getPlatformParameters() {
    return array(
      'Provider' => isset($this->options['meta_provider']) ? $this->options['meta_provider'] : '',
      'Service' => isset($this->options['meta_db_service']) ? $this->options['meta_db_service'] : (isset($this->options['meta_compute_service']) ? $this->options['meta_compute_service'] : ''),
      'Region' => isset($this->options['meta_region']) ? $this->options['meta_region'] : '',
      'Configuration' => isset($this->options['meta_db_service_config']) ? $this->options['meta_db_service_config'] : (isset($this->options['meta_instance_id']) ? $this->options['meta_instance_id'] : ''),
      'CPU' => isset($this->options['meta_cpu']) ? $this->options['meta_cpu'] : '',
      'Memory' => isset($this->options['meta_memory']) ? $this->options['meta_memory'] : '',
      'Operating System' => isset($this->options['meta_os']) ? $this->options['meta_os'] : '',
      'Test ID' => isset($this->options['meta_test_id']) ? $this->options['meta_test_id'] : '',
    );
  }
  
  /**
   * returns results as an array of rows if testing was successful, NULL 
   * otherwise
   * @return array
   */
  public function getResults() {
    $rows = NULL;
    if (is_dir($this->dir) && self::getSerializedOptions($this->dir) && $this->getRunOptions() && isset($this->options['results'])) {
      $this->noRate = isset($this->options['noRate']) && $this->options['noRate'];
      $rows = array();
      $brow = array();
      foreach($this->options as $key => $val) {
        if (!is_array($this->options[$key])) $brow[$key] = $val;
      }
      foreach($this->options['results'] as $result) {
        if (isset($result['warmup']) && $result['warmup']) continue;
        
        if (isset($result['step_started']) && is_numeric($result['step_started'])) $result['step_started'] = date(OltpBenchTest::OLTP_BENCH_DB_DATA_FORMAT, $result['step_started']);
        if (isset($result['step_stopped']) && is_numeric($result['step_stopped'])) $result['step_stopped'] = date(OltpBenchTest::OLTP_BENCH_DB_DATA_FORMAT, $result['step_stopped']);
        $row = array_merge($brow, $result);
        if (isset($row['test_rate']) && $this->noRate) unset($row['test_rate']);
        $rows[] = $row;
      }
    }
    return $rows;
  }
  
  /**
   * returns run options represents as a hash
   * @return array
   */
  public function getRunOptions() {
    if (!isset($this->options)) {
      if ($this->dir) $this->options = self::getSerializedOptions($this->dir);
      else {
        // default run argument values
        $sysInfo = get_sys_info();
        $defaults = array(
          'auctionmark_ratio_get_item' => 45,
          'auctionmark_ratio_get_user_info' => 10,
          'auctionmark_ratio_new_bid' => 20,
          'auctionmark_ratio_new_comment' => 2,
          'auctionmark_ratio_new_comment_response' => 1,
          'auctionmark_ratio_new_feedback' => 4,
          'auctionmark_ratio_new_item' => 10,
          'auctionmark_ratio_new_purchase' => 5,
          'auctionmark_ratio_update_item' => 3,
          'collectd_rrd_dir' => '/var/lib/collectd/rrd',
          'db_host' => 'localhost',
          'db_isolation' => 'serializable',
          'db_load' => FALSE,
          'db_name' => 'oltp_[benchmark]',
          'db_type' => 'mysql',
          'db_user' => 'root',
          'epinions_ratio_get_review_item_id' => 10,
          'epinions_ratio_get_reviews_user' => 10,
          'epinions_ratio_get_average_rating_trusted_user' => 10,
          'epinions_ratio_get_average_rating' => 10,
          'epinions_ratio_get_item_reviews_trusted_user' => 10,
          'epinions_ratio_update_user_name' => 10,
          'epinions_ratio_update_item_title' => 10,
          'epinions_ratio_update_review_rating' => 10,
          'epinions_ratio_update_trust_rating' => 20,
          'font_size' => 9,
          'jpab_ratio_delete' => 25,
          'jpab_ratio_persist' => 25,
          'jpab_ratio_retrieve' => 25,
          'jpab_ratio_update' => 25,
          'jpab_test' => array('basic'),
          'meta_compute_service' => 'Not Specified',
          'meta_cpu' => $sysInfo['cpu'],
          'meta_instance_id' => 'Not Specified',
          'meta_memory' => $sysInfo['memory_gb'] > 0 ? $sysInfo['memory_gb'] . ' GB' : $sysInfo['memory_mb'] . ' MB',
          'meta_os' => $sysInfo['os_info'],
          'meta_provider' => 'Not Specified',
          'meta_storage_config' => 'Not Specified',
          'test' => array('tpcc'),
          'output' => trim(shell_exec('pwd')),
          'resourcestresser_ratio_cpu1' => 17,
          'resourcestresser_ratio_cpu2' => 17,
          'resourcestresser_ratio_io1' => 17,
          'resourcestresser_ratio_io2' => 17,
          'resourcestresser_ratio_contention1' => 16,
          'resourcestresser_ratio_contention2' => 16,
          'seats_ratio_delete_reservation' => 10,
          'seats_ratio_find_flights' => 10,
          'seats_ratio_find_open_seats' => 35,
          'seats_ratio_new_reservation' => 20,
          'seats_ratio_update_customer' => 10,
          'seats_ratio_update_reservation' => 15,
          'steady_state_threshold' => 5,
          'steady_state_window' => 3,
          'tatp_ratio_delete_call_forwarding' => 2,
          'tatp_ratio_get_access_data' => 35,
          'tatp_ratio_get_new_destination' => 10,
          'tatp_ratio_get_subscriber_data' => 35,
          'tatp_ratio_insert_call_forwarding' => 2,
          'tatp_ratio_update_location' => 14,
          'tatp_ratio_update_subscriber_data' => 2,
          'test_clients' => isset($sysInfo['cpu_cores']) ? $sysInfo['cpu_cores'] : 2,
          'test_processes' => 1,
          'test_sample_interval' => 5,
          'test_time' => 300,
          'tpcc_ratio_delivery' => 4,
          'tpcc_ratio_new_order' => 45,
          'tpcc_ratio_order_status' => 4,
          'tpcc_ratio_payment' => 43,
          'tpcc_ratio_stock_level' => 4,
          'twitter_ratio_get_tweet' => 0.07,
          'twitter_ratio_get_tweet_following' => 0.07,
          'twitter_ratio_get_followers' => 7.6725,
          'twitter_ratio_get_user_tweets' => 91.2656,
          'twitter_ratio_insert_tweet' => 0.9219,
          'wikipedia_ratio_add_watch_list' => 0.07,
          'wikipedia_ratio_remove_watch_list' => 0.07,
          'wikipedia_ratio_update_page' => 7.6725,
          'wikipedia_ratio_get_page_anonymous' => 91.2656,
          'wikipedia_ratio_get_page_authenticated' => 0.9219,
          'ycsb_ratio_read' => 50,
          'ycsb_ratio_insert' => 50,
          'ycsb_ratio_scan' => 0,
          'ycsb_ratio_update' => 0,
          'ycsb_ratio_delete' => 0,
          'ycsb_ratio_read_modify_write' => 0
        );
        $opts = array(
          'auctionmark_customers:',
          'auctionmark_ratio_get_item:',
          'auctionmark_ratio_get_user_info:',
          'auctionmark_ratio_new_bid:',
          'auctionmark_ratio_new_comment:',
          'auctionmark_ratio_new_comment_response:',
          'auctionmark_ratio_new_feedback:',
          'auctionmark_ratio_new_item:',
          'auctionmark_ratio_new_purchase:',
          'auctionmark_ratio_update_item:',
          'epinions_ratio_get_review_item_id:',
          'epinions_ratio_get_reviews_user:',
          'epinions_ratio_get_average_rating_trusted_user:',
          'epinions_ratio_get_average_rating:',
          'epinions_ratio_get_item_reviews_trusted_user:',
          'epinions_ratio_update_user_name:',
          'epinions_ratio_update_item_title:',
          'epinions_ratio_update_review_rating:',
          'epinions_ratio_update_trust_rating:',
          'epinions_users:',
          'classpath:',
          'collectd_rrd',
          'collectd_rrd_dir:',
          'db_create',
          'db_driver:',
          'db_dump:',
          'db_host:',
          'db_isolation:',
          'db_load',
          'db_load_only',
          'db_name:',
          'db_nodrop',
          'db_port:',
          'db_pswd:',
          'db_type:',
          'db_url:',
          'db_user:',
          'font_size:',
          'jpab_objects:',
          'jpab_ratio_delete:',
          'jpab_ratio_persist:',
          'jpab_ratio_retrieve:',
          'jpab_ratio_update:',
          'jpab_test:',
          'meta_db_service:',
          'meta_db_service_id:',
          'meta_db_service_config:',
          'meta_compute_service:',
          'meta_compute_service_id:',
          'meta_cpu:',
          'meta_instance_id:',
          'meta_memory:',
          'meta_os:',
          'meta_provider:',
          'meta_provider_id:',
          'meta_region:',
          'meta_resource_id:',
          'meta_run_id:',
          'meta_run_group_id:',
          'meta_storage_config:',
          'meta_test_id:',
          'nopdfreport',
          'noreport',
          'output:',
          'reportdebug',
          'test:',
          'resourcestresser_ratio_cpu1:',
          'resourcestresser_ratio_cpu2:',
          'resourcestresser_ratio_io1:',
          'resourcestresser_ratio_io2:',
          'resourcestresser_ratio_contention1:',
          'resourcestresser_ratio_contention2:',
          'seats_customers:',
          'seats_ratio_delete_reservation:',
          'seats_ratio_find_flights:',
          'seats_ratio_find_open_seats:',
          'seats_ratio_new_reservation:',
          'seats_ratio_update_customer:',
          'seats_ratio_update_reservation:',
          'steady_state_threshold:',
          'steady_state_window:',
          'tatp_ratio_delete_call_forwarding:',
          'tatp_ratio_get_access_data:',
          'tatp_ratio_get_new_destination:',
          'tatp_ratio_get_subscriber_data:',
          'tatp_ratio_insert_call_forwarding:',
          'tatp_ratio_update_location:',
          'tatp_ratio_update_subscriber_data:',
          'tatp_subscribers:',
          'test_clients:',
          'test_clients_step:',
          'test_idle',
          'test_processes:',
          'test_rate:',
          'test_rate_step:',
          'test_sample_interval:',
          'test_size:',
          'test_size_ratio:',
          'test_time:',
          'test_time_step:',
          'test_warmup',
          'tpcc_ratio_delivery:',
          'tpcc_ratio_new_order:',
          'tpcc_ratio_order_status:',
          'tpcc_ratio_payment:',
          'tpcc_ratio_stock_level:',
          'tpcc_warehouses:',
          'twitter_ratio_get_tweet:',
          'twitter_ratio_get_tweet_following:',
          'twitter_ratio_get_followers:',
          'twitter_ratio_get_user_tweets:',
          'twitter_ratio_insert_tweet:',
          'twitter_users:',
          'v' => 'verbose',
          'wikipedia_pages:',
          'wikipedia_ratio_add_watch_list:',
          'wikipedia_ratio_remove_watch_list:',
          'wikipedia_ratio_update_page:',
          'wikipedia_ratio_get_page_anonymous:',
          'wikipedia_ratio_get_page_authenticated:',
          'ycsb_user_rows:',
          'ycsb_ratio_read:',
          'ycsb_ratio_insert:',
          'ycsb_ratio_scan:',
          'ycsb_ratio_update:',
          'ycsb_ratio_delete:',
          'ycsb_ratio_read_modify_write:'
        );
        $this->options = parse_args($opts, array('jpab_test', 'test'));
        $this->verbose = isset($this->options['verbose']);
        
        // dynamic size values
        if (isset($this->options['test_size']) && 
           ($bmb = preg_match('/^\//', trim($this->options['test_size'])) ? get_free_space($this->options['test_size']) : size_from_string($this->options['test_size']))) {
          $mb = $bmb;
          if (!isset($this->options['test_size_ratio'])) $this->options['test_size_ratio'] = preg_match('/^\//', trim($this->options['test_size'])) ? 90 : 100;
          if ($this->options['test_size_ratio'] >= 1 && $this->options['test_size_ratio'] <= 100) $mb = $mb*($this->options['test_size_ratio']*0.01);
          print_msg(sprintf('Got free space %s MB from --test_size %s [%s MB] and --test_size_ratio %d', round($mb, 2), $this->options['test_size'], round($bmb, 2), $this->options['test_size_ratio']), $this->verbose, __FILE__, __LINE__);
          
          foreach(array('auctionmark_customers', 'epinions_users', 'seats_customers', 'tatp_subscribers', 'tpcc_warehouses', 'twitter_users', 'wikipedia_pages', 'ycsb_user_rows') as $p) {
            if (!isset($this->options[$p])) {
              $v = round($this->mbToParam($mb, $p));
              $this->options[$p] = $v;
              print_msg(sprintf('Set parameter %s=%d from --test_size %s [%d MB]', $p, $v, $this->options['test_size'], $mb), $this->verbose, __FILE__, __LINE__);
            }
          }
        }
        
        // set default values
        foreach($defaults as $key => $val) {
          if (!isset($this->options[$key])) $this->options[$key] = $val;
        }
        if (!isset($this->options['test_rate'])) {
          $this->options['test_rate'] = 10000;
          $this->noRate = TRUE;
        }
        
        // all tests
        if (in_array('all', $this->options['test'])) $this->options['test'] = array('auctionmark', 'epinions', 'jpab', 'resourcestresser', 'seats', 'tatp', 'tpcc', 'twitter', 'wikipedia', 'ycsb');
        
        // all JPAB subtests
        if (in_array('all', $this->options['jpab_test'])) $this->options['jpab_test'] = array('basic', 'collection', 'inheritance', 'indexing', 'graph');
        
        // create database
        if (isset($this->options['db_load']) && !isset($this->options['db_create'])) $this->options['db_create'] = TRUE;
        
        // database driver
        if (!isset($this->options['db_driver']) && $this->options['db_type'] == 'mysql') $this->options['db_driver'] = 'com.mysql.jdbc.Driver';
        if (!isset($this->options['db_driver']) && $this->options['db_type'] == 'postgres') $this->options['db_driver'] = 'org.postgresql.Driver';
        
        // database port
        if (!isset($this->options['db_port']) && $this->options['db_type'] == 'mysql') $this->options['db_port'] = 3306;
        if (!isset($this->options['db_port']) && $this->options['db_type'] == 'postgres') $this->options['db_port'] = 5432;
        
        // database JDBC URL
        if (!isset($this->options['db_url'])) $this->options['db_url'] = sprintf('jdbc:%s://%s%s/%s', $this->options['db_type'] == 'postgres' ? 'postgresql' : $this->options['db_type'], $this->options['db_host'], isset($this->options['db_port']) ? ':' . $this->options['db_port'] : '', $this->options['db_name']);
        
        // substitute [cpus]
        $this->options['test_clients'] = str_replace('[cpus]', isset($sysInfo['cpu_cores']) ? $sysInfo['cpu_cores'] : 2, $this->options['test_clients']);
        
        foreach(array('test_clients', 'test_rate', 'test_time') as $p) {
          $vals = array();
          $pstep = $p . '_step';
          if (preg_match('/^([0-9]+)\s*\-\s*([0-9]+)$/', trim($this->options[$p]), $m) && $m[1] < $m[2]) {
            $vals[] = $m[1]*1;
            $diff = $m[2] - $m[1];
            $step = isset($this->options[$pstep]) && $this->options[$pstep] > 0 && $this->options[$pstep] < $diff ? $this->options[$pstep] : $diff;
            while($vals[count($vals) - 1] < $m[2]) $vals[] = $vals[count($vals) - 1] + $step;
          }
          else if (preg_match('/,/', $this->options[$p])) {
            foreach(explode(',', trim($this->options[$p])) as $v) if (is_numeric($v) && $v > 0) $vals[] = $v*1; 
          }
          else if (is_numeric($this->options[$p])) $vals[] = $this->options[$p]*1;
          $this->options[$p] = $vals;
        }
        
        if ($this->options['test_clients'] && $this->options['test_rate'] && $this->options['test_time']) {
          // determine max number of clients
          foreach($this->options['test_clients'] as $c) if ($c > $this->maxClients) $this->maxClients = $c;
          if (!isset($this->options['tpcc_warehouses'])) $this->options['tpcc_warehouses'] = $this->maxClients;
          if (!isset($this->options['auctionmark_customers'])) $this->options['auctionmark_customers'] = $this->maxClients*1000;
          if (!isset($this->options['epinions_users'])) $this->options['epinions_users'] = $this->maxClients*20000;
          if (!isset($this->options['jpab_objects'])) $this->options['jpab_objects'] = $this->maxClients*100000;
          if (!isset($this->options['seats_customers'])) {
            $this->options['seats_customers'] = $this->maxClients*100;
            if ($this->options['seats_customers'] < 1000) $this->options['seats_customers'] = 1000;
          }
          if (!isset($this->options['tatp_subscribers'])) $this->options['tatp_subscribers'] = $this->maxClients*10;
          if (!isset($this->options['twitter_users'])) $this->options['twitter_users'] = $this->maxClients*500;
          if (!isset($this->options['wikipedia_pages'])) $this->options['wikipedia_pages'] = $this->maxClients*1000;
          if (!isset($this->options['ycsb_user_rows'])) $this->options['ycsb_user_rows'] = $this->maxClients*10000;
          
          // determine number of steps
          $steps = 0;
          foreach(array('test_clients', 'test_rate', 'test_time') as $p) if (count($this->options[$p]) > $steps) $steps = count($this->options[$p]);
          
          $this->options['test_processes'] = $this->evaluateExpression($this->options['test_processes']);
          
          // create steps
          $stepStart = 0;
          $maxMins = 0;
          for($i = 0; $i<$steps; $i++) {
            $this->steps[$i] = array('clients' => isset($this->options['test_clients'][$i]) ? $this->options['test_clients'][$i] : $this->options['test_clients'][$steps % count($this->options['test_clients'])],
                                     'rate' => isset($this->options['test_rate'][$i]) ? $this->options['test_rate'][$i] : $this->options['test_rate'][$steps % count($this->options['test_rate'])],
                                     'start' => $stepStart,
                                     'time' => isset($this->options['test_time'][$i]) ? $this->options['test_time'][$i] : $this->options['test_time'][$steps % count($this->options['test_time'])]);
            $this->steps[$i]['stop'] = $this->steps[$i]['start'] + $this->steps[$i]['time'];
            $stepStart = $this->steps[$i]['stop'];
            if (($fl = floor($this->steps[$i]['time']/60)) > $maxMins) $maxMins = $fl;
            print_msg(sprintf('Added test step; processes=%d; clients=%d; rate=%d req/sec; start=%d secs; stop=%d secs; time=%d secs', 
                              $this->options['test_processes'], 
                              $this->steps[$i]['clients'], 
                              $this->steps[$i]['rate'], 
                              $this->steps[$i]['start'], 
                              $this->steps[$i]['stop'], 
                              $this->steps[$i]['time']), 
                              $this->verbose, __FILE__, __LINE__);
          }
        }
        
        // repeat first step for warmup
        if (isset($this->options['test_warmup'])) {
          $steps = array($this->steps[0]);
          foreach($this->steps as $step) $steps[] = $step;
          $this->steps = $steps;
          print_msg(sprintf('Duplicated first step for warmup - total steps = %d', count($this->steps)), $this->verbose, __FILE__, __LINE__);
        }
        
        // reduce steady_state_window if it is larger than the longest test step
        if ($maxMins < 1) $maxMins = 1;
        if ($maxMins < $this->options['steady_state_window']) {
          print_msg(sprintf('Reducing steady_state_window from %d to %d - the max duration of any given test', $this->options['steady_state_window'], $maxMins), $this->verbose, __FILE__, __LINE__);
          $this->options['steady_state_window'] = $maxMins;
        }
        
        // database dump file
        if (isset($this->options['db_dump']) && is_dir($this->options['db_dump'])) $this->options['db_dump'] = str_replace('//', '/', $this->options['db_dump'] . '/oltp-[benchmark]-[subtest]-[scalefactor]-[db_type].sql');
        if (isset($this->options['db_dump'])) $this->options['db_dump'] = str_replace('[db_type]', $this->options['db_type'], $this->options['db_dump']);
        
        // mysql or postgres
        $this->mysqlOrPostgres = $this->options['db_type'] == 'mysql' || $this->options['db_type'] == 'postgres';
        
      }
    }
    return $this->options;
  }
  
  /**
   * returns the scale factor to use for this test
   * @return int
   */
  private function getScaleFactor() {
    $factor = NULL;
    switch($this->test) {
      case 'auctionmark':
        $factor = round($this->options['auctionmark_customers']/1000);
        break;
      case 'epinions':
        $factor = round($this->options['epinions_users']/2000);
        break;
      case 'jpab':
        $factor = $this->options['jpab_objects']*1;
        break;
      case 'seats':
        $factor = round($this->options['seats_customers']/1000);
        break;
      case 'tatp':
        $factor = $this->options['tatp_subscribers']*1;
        break;
      case 'tpcc':
        $factor = $this->options['tpcc_warehouses']*1;
        break;
      case 'twitter':
        $factor = round($this->options['twitter_users']/1000);
        break;
      case 'wikipedia':
        $factor = round($this->options['wikipedia_pages']/1000);
        break;
      case 'ycsb':
        $factor = round($this->options['ycsb_user_rows']/1000);
        break;
    }
    return $factor;
  }
  
  /**
   * returns options from the serialized file where they are written when a 
   * test completes
   * @param string $dir the directory where results were written to
   * @return array
   */
  public static function getSerializedOptions($dir) {
    $ofile = sprintf('%s/%s', $dir, self::OLTP_BENCH_TEST_OPTIONS_FILE_NAME);
    return file_exists($ofile) ? unserialize(file_get_contents($ofile)) : NULL;
  }
  
  /**
   * returns the parameter used for sizing fo test $test. returns NULL for 
   * resourcestresser
   * @param string $test the test to return the parameter for
   * @param boolean $value return the parameter value (otherwise name is 
   * returned)?
   * @return mixed
   */
  private function getSizeParam($test, $value=FALSE) {
    $param = NULL;
    switch($this->test) {
      case 'auctionmark':
        $param = 'auctionmark_customers';
        break;
      case 'epinions':
        $param = 'epinions_users';
        break;
      case 'jpab':
        $param = 'jpab_objects';
        break;
      case 'seats':
        $param = 'seats_customers';
        break;
      case 'tatp':
        $param = 'tatp_subscribers';
        break;
      case 'tpcc':
        $param = 'tpcc_warehouses';
        break;
      case 'twitter':
        $param = 'twitter_users';
        break;
      case 'wikipedia':
        $param = 'wikipedia_pages';
        break;
      case 'ycsb':
        $param = 'ycsb_user_rows';
        break;
    }
    return $value && $param ? (isset($this->options[$param]) ? $this->options[$param] : NULL) : $param;
  }
  
  /**
   * returns the label to use when representing the size metric for $test
   * @param string $test the test to return the size label for
   * @return string
   */
  private function getSizeParamLabel($test) {
    $label = NULL;
    switch($this->test) {
      case 'auctionmark':
      case 'seats':
        $label = 'Customers';
        break;
      case 'epinions':
      case 'twitter':
        $label = 'Users';
        break;
      case 'jpab':
        $label = 'Objects';
        break;
      case 'tatp':
        $label = 'Subscribers';
        break;
      case 'tpcc':
        $label = 'Warehouses';
        break;
      case 'wikipedia':
        $label = 'Pages';
        break;
      case 'ycsb':
        $label = 'User Rows';
        break;
    }
    return $label;
  }
  
  /**
   * returns the sub-tests associated with the current test
   * @return array
   */
  private function getSubTests() {
    $subtests = array();
    switch($this->test) {
      case 'jpab':
        $subtests = $this->options['jpab_test'];
        break;
      default:
        $subtests[] = $this->test;
        break;
    }
    return $subtests;
  }
  
  /**
   * returns the test weights to use as an array of values
   * @param string $test optional test to return weight for - otherwise 
   * returned for $this->test
   * @return array
   */
  private function getTestWeights($test=NULL) {
    $weights = array();
    $params = NULL;
    switch($test ? $test : $this->test) {
      case 'auctionmark':
        $params = array('auctionmark_ratio_get_item',
                        'auctionmark_ratio_get_user_info',
                        'auctionmark_ratio_new_bid',
                        'auctionmark_ratio_new_comment',
                        'auctionmark_ratio_new_comment_response',
                        'auctionmark_ratio_new_feedback',
                        'auctionmark_ratio_new_item',
                        'auctionmark_ratio_new_purchase',
                        'auctionmark_ratio_update_item');
        break;
      case 'epinions':
        $params = array('epinions_ratio_get_review_item_id',
                        'epinions_ratio_get_reviews_user',
                        'epinions_ratio_get_average_rating_trusted_user',
                        'epinions_ratio_get_average_rating',
                        'epinions_ratio_get_item_reviews_trusted_user',
                        'epinions_ratio_update_user_name',
                        'epinions_ratio_update_item_title',
                        'epinions_ratio_update_review_rating',
                        'epinions_ratio_update_trust_rating');
        break;
      case 'jpab':
        $params = array('jpab_ratio_delete',
                        'jpab_ratio_persist',
                        'jpab_ratio_retrieve',
                        'jpab_ratio_update');
        break;
      case 'resourcestresser':
        $params = array('resourcestresser_ratio_cpu1',
                        'resourcestresser_ratio_cpu2',
                        'resourcestresser_ratio_io1',
                        'resourcestresser_ratio_io2',
                        'resourcestresser_ratio_contention1',
                        'resourcestresser_ratio_contention2');
        break;
      case 'seats':
        $params = array('seats_ratio_delete_reservation',
                        'seats_ratio_find_flights',
                        'seats_ratio_find_open_seats',
                        'seats_ratio_new_reservation',
                        'seats_ratio_update_customer',
                        'seats_ratio_update_reservation');
        break;
      case 'tatp':
        $params = array('tatp_ratio_delete_call_forwarding',
                        'tatp_ratio_get_access_data',
                        'tatp_ratio_get_new_destination',
                        'tatp_ratio_get_subscriber_data',
                        'tatp_ratio_insert_call_forwarding',
                        'tatp_ratio_update_location',
                        'tatp_ratio_update_subscriber_data');
        break;
      case 'tpcc':
        $params = array('tpcc_ratio_delivery',
                        'tpcc_ratio_new_order',
                        'tpcc_ratio_order_status',
                        'tpcc_ratio_payment',
                        'tpcc_ratio_stock_level');
        break;
      case 'twitter':
        $params = array('twitter_ratio_get_tweet',
                        'twitter_ratio_get_tweet_following',
                        'twitter_ratio_get_followers',
                        'twitter_ratio_get_user_tweets',
                        'twitter_ratio_insert_tweet');
        break;
      case 'wikipedia':
        $params = array('wikipedia_ratio_add_watch_list',
                        'wikipedia_ratio_remove_watch_list',
                        'wikipedia_ratio_update_page',
                        'wikipedia_ratio_get_page_anonymous',
                        'wikipedia_ratio_get_page_authenticated');
        break;
      case 'ycsb':
        $params = array('ycsb_ratio_read',
                        'ycsb_ratio_insert',
                        'ycsb_ratio_scan',
                        'ycsb_ratio_update',
                        'ycsb_ratio_delete',
                        'ycsb_ratio_read_modify_write');
        break;
    }
    if ($params) {
      foreach($params as $p) $weights[] = isset($this->options[$p]) ? $this->options[$p] : 0;
    }
    return $weights;
  }
  
  /**
   * returns the transaction types for this test as an ordered array of name/id 
   * pairs. If ID is FALSE, it will not be included in the config
   * @return array
   */
  private function getTransactionTypes() {
    $types = array();
    switch($this->test) {
      case 'auctionmark':
        $types['GetItem'] = FALSE;
        $types['GetUserInfo'] = FALSE;
        $types['NewBid'] = FALSE;
        $types['NewComment'] = FALSE;
        $types['NewCommentResponse'] = FALSE;
        $types['NewFeedback'] = FALSE;
        $types['NewItem'] = FALSE;
        $types['NewPurchase'] = FALSE;
        $types['UpdateItem'] = FALSE;
        break;
      case 'epinions':
        $types['GetReviewItemById'] = FALSE;
        $types['GetReviewsByUser'] = FALSE;
        $types['GetAverageRatingByTrustedUser'] = FALSE;
        $types['GetItemAverageRating'] = FALSE;
        $types['GetItemReviewsByTrustedUser'] = FALSE;
        $types['UpdateUserName'] = FALSE;
        $types['UpdateItemTitle'] = FALSE;
        $types['UpdateReviewRating'] = FALSE;
        $types['UpdateTrustRating'] = FALSE;
        break;
      case 'jpab':
        $types['Persist'] = FALSE;
        $types['Retrieve'] = FALSE;
        $types['Update'] = FALSE;
        $types['Delete'] = FALSE;
        break;
      case 'resourcestresser':
        $types['CPU1'] = FALSE;
        $types['CPU2'] = FALSE;
        $types['IO1'] = FALSE;
        $types['IO2'] = FALSE;
        $types['Contention1'] = FALSE;
        $types['Contention2'] = FALSE;
        break;
      case 'seats':
        $types['DeleteReservation'] = FALSE;
        $types['FindFlights'] = FALSE;
        $types['FindOpenSeats'] = FALSE;
        $types['NewReservation'] = FALSE;
        $types['UpdateCustomer'] = FALSE;
        $types['UpdateReservation'] = FALSE;
        break;
      case 'tatp':
        $types['DeleteCallForwarding'] = FALSE;
        $types['GetAccessData'] = FALSE;
        $types['GetNewDestination'] = FALSE;
        $types['GetSubscriberData'] = FALSE;
        $types['InsertCallForwarding'] = FALSE;
        $types['UpdateLocation'] = FALSE;
        $types['UpdateSubscriberData'] = FALSE;
        break;
      case 'tpcc':
        $types['NewOrder'] = FALSE;
        $types['Payment'] = FALSE;
        $types['OrderStatus'] = FALSE;
        $types['Delivery'] = FALSE;
        $types['StockLevel'] = FALSE;
        break;
      case 'twitter':
        $types['GetTweet'] = FALSE;
        $types['GetTweetsFromFollowing'] = FALSE;
        $types['GetFollowers'] = FALSE;
        $types['GetUserTweets'] = FALSE;
        $types['InsertTweet'] = FALSE;
        break;
      case 'wikipedia':
        $types['AddWatchList'] = FALSE;
        $types['RemoveWatchList'] = FALSE;
        $types['UpdatePage'] = FALSE;
        $types['GetPageAnonymous'] = FALSE;
        $types['GetPageAuthenticated'] = FALSE;
        break;
      case 'ycsb':
        $types['ReadRecord'] = FALSE;
        $types['InsertRecord'] = FALSE;
        $types['ScanRecord'] = FALSE;
        $types['UpdateRecord'] = FALSE;
        $types['DeleteRecord'] = FALSE;
        $types['ReadModifyWriteRecord'] = FALSE;
        break;
    }
    return $types;
  }
  
  /**
   * make coordinates tuples from a results array
   * @param array $vals results array (indexed by seconds)
   * @param boolean $histogram make coordinates for a histogram
   * @param boolean $secsReset if TRUE, seconds will be explictly set to start 
   * at 0 and jump by test_sample_interval
   * @return array
   */
  private function makeCoords($vals, $histogram=FALSE, $secsReset=FALSE) {
    $coords = array();
    if ($histogram) {
      $min = NULL;
      $max = NULL;
      foreach($vals as $val) {
        if (!isset($min) || $val < $min) $min = $val;
        if (!isset($max) || $val > $max) $max = $val;        
      }
      $min = floor($min/100)*100;
      $max = ceil($max/100)*100;
      $diff = $max - $min;
      $step = round($diff/8);
      for($start=$min; $start<$max; $start+=$step) {
        $label = sprintf('%d', $start);
        $coords[$label] = 0;
        foreach($vals as $val) if ($val >= $start && $val < ($start + $step)) $coords[$label]++;
        $coords[$label] = array($coords[$label]);
      }
    }
    else {
      foreach(array_keys($vals) as $i => $secs) $coords[] = array($secsReset ? $i*$this->options['test_sample_interval'] : $secs, $vals[$secs]);
    }
    return $coords;
  }
  
  /**
   * converts $mb megabytes to the associated $param value
   * @param float $mb the megabytes to convert with
   * @param string $param the param to convert
   * @return int
   */
  private function mbToParam($mb, $param) {
    $val = NULL;
    switch($param) {
      case 'auctionmark_customers':
        $val = 1000*round($mb/160);
        if ($val < 1000) $val = 1000;
        break;
      case 'epinions_users':
        $val = 2000*round($mb/30);
        if ($val < 2000) $val = 2000;
        break;
      case 'seats_customers':
        $val = 1000*round($mb/180);
        if ($val < 1000) $val = 1000;
        break;
      case 'tatp_subscribers':
        $val = round($mb/95);
        if ($val < 1) $val = 1;
        break;
      case 'tpcc_warehouses':
        $val = round($mb/110);
        if ($val < 1) $val = 1;
        break;
      case 'twitter_users':
        $val = 1000*round($mb/8);
        if ($val < 1000) $val = 1000;
        break;
      case 'wikipedia_pages':
        $val = 1000*round($mb/300);
        if ($val < 1000) $val = 1000;
        break;
      case 'ycsb_user_rows':
        $val = 1000*round($mb/4);
        if ($val < 1000) $val = 1000;
        break;
    }
    return $val;
  }
  
  /**
   * converts the parameter value $val to it's associated size in megabytes
   * @param int $val the parameter value
   * @param string $param the param to convert
   * @return int
   */
  private function paramToMb($val, $param) {
    $mb = NULL;
    switch($param) {
      case 'auctionmark_customers':
        if ($val < 1000) $val = 1000;
        $mb = round($val/1000)*160;
        break;
      case 'epinions_users':
        if ($val < 2000) $val = 2000;
        $mb = round($val/2000)*30;
        break;
      case 'seats_customers':
        if ($val < 1000) $val = 1000;
        $mb = round($val/1000)*180;
        break;
      case 'tatp_subscribers':
        if ($val < 1) $val = 1;
        $mb = $val*95;
        break;
      case 'tpcc_warehouses':
        if ($val < 1) $val = 1;
        $mb = $val*110;
        break;
      case 'twitter_users':
        if ($val < 1000) $val = 1000;
        $mb = round($val/1000)*8;
        break;
      case 'wikipedia_pages':
        if ($val < 1000) $val = 1000;
        $mb = round($val/1000)*300;
        break;
      case 'ycsb_user_rows':
        if ($val < 1000) $val = 1000;
        $mb = round($val/1000)*4;
        break;
    }
    return $mb;
  }
  
  /**
   * returns TRUE if OLTP-Bench has been built already
   * @return boolean
   */
  private static function oltpBenchIsBuilt() {
    return file_exists(dirname(__FILE__) . '/oltpbench/build/com/oltpbenchmark/DBWorkload.class');
  }
  
  /**
   * initiates OltpBench testing. returns the number of tests completed 
   * successfully
   * @return int
   */
  public function test() {
    $success = 0;
    $this->getRunOptions();
    
    foreach($this->options['test'] as $test) {
      $rrdStarted = isset($this->options['collectd_rrd']) ? ch_collectd_rrd_start($this->options['collectd_rrd_dir'], $this->verbose) : FALSE;
      $this->test = $test;
      $dbName = str_replace('[benchmark]', $test, $this->options['db_name']);
      foreach($this->getSubTests() as $subtest) {
        $this->subtest = $subtest;
        $testId = $test . ($test != $subtest ? '-' . $subtest : '');
        $dbLoadTime = NULL;
        $dbLoadFromDump = FALSE;
        if ($config = $this->buildOltpBenchConfig()) {
          
          // idle test mode
          if (isset($this->options['test_idle'])) {
            foreach($this->steps as $step) {
              print_msg(sprintf('Sleeping for %d seconds for test %s due to --test_idle flag', $step['time'], $testId), $this->verbose, __FILE__, __LINE__);
              sleep($step['time']);
              $success++;
            }
            continue;
          }
          
          $dropDbCmd = NULL;
          print_msg(sprintf('Starting test %s with %d processes', $testId, $this->options['test_processes']), $this->verbose, __FILE__, __LINE__);
          
          // create and load database
          if (!isset($this->options['reportdebug']) && isset($this->options['db_load'])) {
            $bcmd = NULL;
            if ($this->mysqlOrPostgres) {
              $c = $this->options['db_type'] == 'mysql' ? 'mysql' : 'psql';
              if (!validate_dependencies(array($c => $c))) {
                $bcmd = sprintf('%s%s -h %s %s -%s %s %s',
                               $this->options['db_type'] == 'postgres' && $this->options['db_pswd'] ? 'export PGPASSWORD=' . $this->options['db_pswd'] . '; ' : '',
                               $c,
                               $this->options['db_host'],
                               isset($this->options['db_port']) ? ($this->options['db_type'] == 'mysql' ? '-P ' : '-p ') . $this->options['db_port'] : '',
                               $this->options['db_type'] == 'mysql' ? 'u' : 'U',
                               $this->options['db_user'],
                               $this->options['db_type'] == 'mysql' && $this->options['db_pswd'] ? '-p"' . $this->options['db_pswd'] . '"' : '');
              }
            }
            // attempt to drop and create database
            if ($bcmd && isset($this->options['db_create'])) {
              foreach(array('drop', 'create') as $q) {
                $cmd = sprintf('%s%s %s "%s database %s" >/dev/null 2>&1', 
                               $bcmd, 
                               $this->options['db_type'] == 'postgres' ? ' postgres' : '', $this->options['db_type'] == 'mysql' ? '-e' : '-c', $q, $dbName);
                if (!$dropDbCmd) $dropDbCmd = $cmd;
                $ecode = trim(exec($cmd));
                if ($ecode > 0) print_msg(sprintf('Unable to %s database %s - exit code %d', $q, $dbName, $ecode), $this->verbose, __FILE__, __LINE__, TRUE);
                else print_msg(sprintf('Successfully executed %s database %s', $q, $dbName), $this->verbose, __FILE__, __LINE__);
              }
            }
            $load = TRUE;
            // if test and sub-test are the same, replace string with just test
            if ($dump = isset($this->options['db_dump']) ? str_replace('[scalefactor]', $this->getScaleFactor(), str_replace('[subtest]', $subtest, str_replace('[benchmark]', $test, $this->options['db_dump']))) : NULL) $dump = str_replace(sprintf('%s-%s', $test, $test), $test, $dump);
            
            if ($bcmd && $dump && file_exists($dump) && $this->mysqlOrPostgres) {
              $cmd = sprintf('%s %s < %s', $bcmd, $dbName, $dump);
              print_msg(sprintf('Attempting to import existing database dump file (size %s MB) - this may take a while', round((filesize($dump)/1024)/1024, 2)), $this->verbose, __FILE__, __LINE__);
              $start = time();
              $ecode = trim(exec($cmd));
              if ($ecode > 0) print_msg(sprintf('Failed to load database - exit code %d', $ecode), $this->verbose, __FILE__, __LINE__, TRUE);
              else {
                print_msg(sprintf('Successfully loaded database from dump file %s', $dump), $this->verbose, __FILE__, __LINE__);
                $dbLoadFromDump = TRUE;
                $dbLoadTime = time() - $start;
                $load = FALSE;
              }
            }
            else if ($dump && $this->mysqlOrPostgres) print_msg(sprintf('Database dump file %s does not exist for load', $dump), $this->verbose, __FILE__, __LINE__);
            
            if ($load) {
              $cmd = sprintf('cd %s && ./oltpbenchmark -b %s -c %s%s --load=true -o %s/%s >>%s/%s-load.out 2>>%s/%s-load.err', 
                            dirname(__FILE__) . '/oltpbench', 
                            $test, 
                            $config,
                            isset($this->options['db_create']) ? ' --create=true' : '',
                            $this->options['output'],
                            $testId,
                            $this->options['output'],
                            $testId,
                            $this->options['output'],
                            $testId);
              print_msg(sprintf('Loading database with test data - this may take a while'), $this->verbose, __FILE__, __LINE__);
              $start = time();
              passthru($cmd);
              $dbLoadTime = time() - $start;
              if (isset($this->options['db_dump']) && $this->mysqlOrPostgres) {
                $cmd = sprintf('%s%s -h %s %s -%s %s %s %s >> %s',
                               $this->options['db_type'] == 'postgres' && $this->options['db_pswd'] ? 'export PGPASSWORD=' . $this->options['db_pswd'] . '; ' : '',
                               $this->options['db_type'] == 'mysql' ? 'mysqldump' : 'pg_dump',
                               $this->options['db_host'],
                               isset($this->options['db_port']) ? ($this->options['db_type'] == 'mysql' ? '-P ' : '-p ') . $this->options['db_port'] : '',
                               $this->options['db_type'] == 'mysql' ? 'u' : 'U',
                               $this->options['db_user'],
                               $this->options['db_type'] == 'mysql' && $this->options['db_pswd'] ? '-p"' . $this->options['db_pswd'] . '"' : '-w -c',
                               $dbName,
                               $dump);
                print_msg(sprintf('Attempting to export database dump to file - this may take a while'), $this->verbose, __FILE__, __LINE__);
                $ecode = trim(exec($cmd));
                if (!file_exists($dump) || !filesize($dump) || $ecode > 0) {
                  exec(sprintf('rm -f %s', $dump));
                  print_msg(sprintf('Failed to dump database - exit code %d', $ecode), $this->verbose, __FILE__, __LINE__, TRUE);
                }
                else {
                  print_msg(sprintf('Successfully dumped database - export file size is %s MB', round((filesize($dump)/1024)/1024, 2)), $this->verbose, __FILE__, __LINE__);
                  $load = FALSE;
                }
              }
            }
          }

          if (!isset($this->options['db_load_only'])) {
            if (!isset($this->options['reportdebug'])) {
              // generate execution script
              $fp = fopen($script = sprintf('%s/%s.sh', $this->options['output'], $test), 'w');
              fwrite($fp, "#!/bin/bash\n");
              fwrite($fp, sprintf("cd %s\n", dirname(__FILE__) . '/oltpbench'));
              for($i=0; $i<$this->options['test_processes']; $i++) {
                $bfile = sprintf('%s/%s-p%d', $this->options['output'], $testId, $i+1);
                exec(sprintf('rm -f %s*', $bfile));
                fwrite($fp, sprintf("nohup ./oltpbenchmark -b %s -c %s --execute=true -s %d -o %s >>%s.out 2>>%s.err &\n",
                      $test, 
                      $config,
                      $this->options['test_sample_interval'],
                      $bfile,
                      $bfile,
                      $bfile));
              }
              fwrite($fp, "wait\n");
              fclose($fp);
              exec(sprintf('chmod 755 %s', $script));

              print_msg(sprintf('Executing test script %s', $script), $this->verbose, __FILE__, __LINE__);
              $started = time();
              if (!isset($this->options['test_started'])) $this->options['test_started'] = date(OltpBenchTest::OLTP_BENCH_DB_DATA_FORMAT);
              passthru($script);
            }
            else {
              $started = time();
              print_msg('Skiping test execution due to use of --reportdebug', $this->verbose, __FILE__, __LINE__);
            }
            
            print_msg('Test script execution complete - processing results', $this->verbose, __FILE__, __LINE__);
            
            // --db_create and --db_load were set - drop database
            if (!isset($this->options['db_nodrop']) && $dropDbCmd) {
              $ecode = trim(exec($dropDbCmd));
              if ($ecode > 0) print_msg(sprintf('Unable to drop database %s - exit code %d', $dbName, $ecode), $this->verbose, __FILE__, __LINE__, TRUE);
              else print_msg(sprintf('Successfully dropped database %s', $dbName), $this->verbose, __FILE__, __LINE__);
            }
            
            // process test results
            for($i=0; $i<$this->options['test_processes']; $i++) {
              $bfile = sprintf('%s/%s-p%d', $this->options['output'], $testId, $i+1);
              // don't need this file - its a duplicate of the config
              exec(sprintf('rm -f %s.ben.cnf', $bfile));
              // db.cnf:  RDBMS settings
              // err:     stderr
              // out:     stdout
              // raw:     raw test results (entries for every test client in this process)
              // res:     results per test_sample_interval (aggregated for all test clients)
              // summary: summarized results
              $res = $bfile . '.res';
              if (file_exists($res) && filesize($res)) {
                print_msg(sprintf('Processing results in %s', $res), $this->verbose, __FILE__, __LINE__);
                foreach(file($res) as $x => $line) {
                  if ($x > 0 && trim($line) && count($cols = explode(',', trim($line))) > 1 && is_numeric($cols[0]) && $cols[1] > 0) {
                    $secs = $cols[0];
                    $stepIndex = 0;
                    // determine step index
                    foreach($this->steps as $n => $step) {
                      if ($secs >= $step['start'] && $secs < $step['stop']) $stepIndex = $n;
                    }
                    $key = $testId . '-' . $stepIndex;
                    print_msg(sprintf('Processing results row %s [process=%d; step=%d; clients=%d; time=%d secs; test time=%d secs]', $key, $i+1, $stepIndex+1, $this->steps[$stepIndex]['clients'], $this->steps[$stepIndex]['time'], $secs), $this->verbose, __FILE__, __LINE__);
                    
                    if (!isset($this->results[$key])) {
                      $success++;
                      $this->results[$key] = array('test' => $test,
                                                   'step' => $stepIndex+1,
                                                   'processes' => $this->options['test_processes'],
                                                   'test_clients' => $this->steps[$stepIndex]['clients'],
                                                   'test_time' => $this->steps[$stepIndex]['time'],
                                                   'warmup' => isset($this->options['test_warmup']) && $stepIndex === 0,
                                                   'step_started' => $started + $this->steps[$stepIndex]['start'],
                                                   'step_stopped' => $started + $this->steps[$stepIndex]['stop'],
                                                   'db_name' => $dbName);
                      if ($param = $this->getSizeParam($test)) {
                        $val = $this->getSizeParam($test, TRUE);
                        if ($mb = $this->paramToMb($val, $param)) $this->results[$key]['size'] = $mb;
                        $this->results[$key]['test_size_label'] = strtoupper($test) . ' ' . $this->getSizeParamLabel($test);
                        $this->results[$key]['test_size_value'] = $val;
                      }
                      if ($dbLoadTime) {
                        $this->results[$key]['db_load_from_dump'] = $dbLoadFromDump;
                        $this->results[$key]['db_load_time'] = $dbLoadTime;
                      }
                      if ($test == 'jpab') $this->results[$key]['jpab_test'] = $subtest;
                      $this->results[$key]['test_rate'] = $this->noRate ? NULL : $this->steps[$stepIndex]['rate'];
                    }
                    foreach(array('throughput_values', 
                                  'latency_values', 
                                  'latency_values_min',
                                  'latency_values_25',
                                  'latency_values_50',
                                  'latency_values_75',
                                  'latency_values_90',
                                  'latency_values_95',
                                  'latency_values_99',
                                  'latency_values_max') as $idx => $p) {
                      if (isset($cols[$idx + 1])) {
                        if (!isset($this->results[$key][$p])) $this->results[$key][$p] = array();
                        if (!isset($this->results[$key][$p][$secs])) $this->results[$key][$p][$secs] = array();              
                        $this->results[$key][$p][$secs][] = $cols[$idx + 1]; 
                      }
                    }
                  }
                }
              }
              else print_msg(sprintf('Results file %s does not exist or is empty', $res), $this->verbose, __FILE__, __LINE__, TRUE);
            }
            if (isset($this->results[$testId . '-0'])) print_msg(sprintf('%s test execution successful', $testId), $this->verbose, __FILE__, __LINE__);
            else if ($test != 'resourcestresser') print_msg(sprintf('%s test execution failed or did not produce any results', $testId), $this->verbose, __FILE__, __LINE__, TRUE);
            else print_msg('resourcestresser does not produce metrics', $this->verbose, __FILE__, __LINE__);
          }
          else print_msg(sprintf('%s test execution skipped due to --db_load_only flag', $test), $this->verbose, __FILE__, __LINE__);
        }
        else print_msg(sprintf('Unable to generate OLTP-Bench XML configuration for test %s', $test), $this->verbose, __FILE__, __LINE__, TRUE);
      }
      
      // collectd stats archives are test specific
      if ($rrdStarted) ch_collectd_rrd_stop($this->options['collectd_rrd_dir'], $this->options['output'], $this->verbose);
      if (is_file($archive = sprintf('%s/collectd-rrd.zip', $this->options['output']))) {
        $narchive = str_replace('.zip', '-' . $test . '.zip', $archive);
        exec(sprintf('mv %s %s', $archive, $narchive));
        print_msg(sprintf('Renamed collectd rrd archive from %s to %s', basename($archive), basename($narchive)), $this->verbose, __FILE__, __LINE__);
      }
    }
    
    // complete results calculations
    foreach(array_keys($this->results) as $key) {
      // for each second interval, take the sum for throughput or median for 
      // latency and use the metrics from all seconds intervals for the 
      // aggregate result metrics
      foreach(array( 'throughput_values', 
                     'latency_values', 
                     'latency_values_min',
                     'latency_values_25',
                     'latency_values_50',
                     'latency_values_75',
                     'latency_values_90',
                     'latency_values_95',
                     'latency_values_99',
                     'latency_values_max') as $p) {
        if (isset($this->results[$key][$p])) {
          $metrics = array();
          foreach(array_keys($this->results[$key][$p]) as $secs) {
            if ($this->results[$key][$p][$secs]) {
              $this->results[$key][$p][$secs] = preg_match('/^latency/', $p) ? round(get_percentile($this->results[$key][$p][$secs])) : array_sum($this->results[$key][$p][$secs]);
              $metrics[] = $this->results[$key][$p][$secs];
            }
            else $this->results[$key][$p][$secs] = 0;
          }
          if (in_array($p, array('throughput_values', 'latency_values', 'latency_values_min', 'latency_values_max')) && $metrics) {
            sort($metrics);
            $bkey = str_replace('_values', '', $p);
            if (!preg_match('/_min/', $p)) {
              $this->results[$key][$bkey] = round(get_mean($metrics));
              $this->results[$key][sprintf('%s_stdev', $bkey)] = round(get_std_dev($metrics));
              foreach(array(10, 20, 30, 40, 50, 60, 70, 80, 90, 95, 99) as $perc) {
                $this->results[$key][sprintf('%s_%d', $bkey, $perc)] = round(get_percentile($metrics, $perc, preg_match('/^latency/', $p) ? TRUE : FALSE));
              }
              if (!preg_match('/^latency/', $p)) {
                $this->results[$key][sprintf('%s_min', $bkey)] = $metrics[0];
                $this->results[$key][sprintf('%s_max', $bkey)] = $metrics[count($metrics) - 1];
              }
            }
            if (preg_match('/_min/', $p)) {
              foreach($metrics as $metric) {
                if ($metric > 0) {
                  $this->results[$key][sprintf('%s_min', $bkey)] = $metric;
                  break;
                }
              }
            }
            if (preg_match('/_max/', $p)) $this->results[$key][sprintf('%s_max', $bkey)] = $metrics[count($metrics) - 1];
          }
        }
      }
      // now determine latency at max throughput
      $maxThroughput = 0;
      $maxThroughputSecs = NULL;
      foreach(array_keys($this->results[$key]['throughput_values']) as $secs) {
        if ($this->results[$key]['throughput_values'][$secs] > $maxThroughput) {
          $maxThroughput = $this->results[$key]['throughput_values'][$secs];
          $maxThroughputSecs = $secs;
        }
      }
      if ($maxThroughput) $this->results[$key]['latency_at_max'] = $this->results[$key]['latency_values'][$maxThroughputSecs];
      
      // finally determine if/when steady state was achieved
      $steadyState = array();
      $steadyStateSamples = round(($this->options['steady_state_window']*60)/$this->options['test_sample_interval']);
      // minimum of 5 measurements required
      if ($steadyStateSamples < 1) $steadyStateSamples = 5;
      print_msg(sprintf('Determining if steady state achieved using %d consecutive samples [window=%d mins; sample=%d secs]', $steadyStateSamples, $this->options['steady_state_window'], $this->options['test_sample_interval']), $this->verbose, __FILE__, __LINE__);
      foreach(array_keys($this->results[$key]['throughput_values']) as $secs) {
        $steadyState[] = $this->results[$key]['throughput_values'][$secs];
        if (count($steadyState) >= $steadyStateSamples) {
          if (($std = round(get_std_dev(array_slice($steadyState, count($steadyState) - $steadyStateSamples, $steadyStateSamples), 3))) <= $this->options['steady_state_threshold']) {
            $this->results[$key]['steady_state'] = $secs;
            print_msg(sprintf('Steady state achieved at %d seconds [stdev=%d]', $secs, $std), $this->verbose, __FILE__, __LINE__);
            break; 
          }
          else print_msg(sprintf('Steady state not achieved at %d seconds [stdev=%d; <= %d required]', $secs, $std, $this->options['steady_state_threshold']), $this->verbose, __FILE__, __LINE__);
        }
      }
      if (!isset($this->results[$key]['steady_state'])) print_msg(sprintf('Steady state was not achieved'), $this->verbose, __FILE__, __LINE__);
    }
    
    $this->endTest($success);
    
    return $success;
  }
  
  /**
   * translates database isolation parameters: serializable, repeatable_read, 
   * read_committed or read_uncommitted to the corresponding OLTP-Bench config
   * value
   * @param string $isolution the isolation parameter to translate
   * @return string
   */
  public static function translateDbIsolation($isolation) {
    $config = NULL;
    switch($isolation) {
      case 'serializable':
        $config = 'TRANSACTION_SERIALIZABLE';
        break;
      case 'repeatable_read':
        $config = 'TRANSACTION_REPEATABLE_READ';
        break;
      case 'read_committed':
        $config = 'TRANSACTION_READ_COMMITTED';
        break;
      case 'read_uncommitted':
        $config = 'TRANSACTION_READ_UNCOMMITTED';
        break;
    }
    return $config;
  }
  
  /**
   * validates test dependencies. returns an array containing the missing 
   * dependencies (array is empty if all dependencies are valid)
   * @return array
   */
  public static function validateDependencies($options) {
    $dependencies = array('java' => 'java', 'zip' => 'zip');
    if (!OltpBenchTest::oltpBenchIsBuilt()) {
      $dependencies['ant'] = 'ant';
      $dependencies['javac'] = 'javac';
    }
    // reporting dependencies
    if (!isset($options['noreport']) || !$options['noreport']) {
      $dependencies['gnuplot'] = 'gnuplot';
      if (!isset($options['nopdfreport']) || !$options['nopdfreport']) $dependencies['wkhtmltopdf'] = 'wkhtmltopdf';
    }
    // mysqldump
    if (isset($options['db_dump']) && $options['db_type'] == 'mysql') {
      $dependencies['mysql'] = 'mysql-client';
      $dependencies['mysqldump'] = 'mysql-client';
    }
    // mysqldump
    if (isset($options['db_dump']) && $options['db_type'] == 'postgres') {
      $dependencies['psql'] = 'postgresql-client';
      $dependencies['pg_dump'] = 'postgresql-client';
    }
    return validate_dependencies($dependencies);
  }
  
  /**
   * validate run options. returns an array populated with error messages 
   * indexed by the argument name. If options are valid, the array returned
   * will be empty
   * @return array
   */
  public function validateRunOptions() {
    $options = $this->getRunOptions();
    $validate = array(
      'auctionmark_customers' => array('min' => 1000),
      'auctionmark_ratio_get_item' => array('min' => 0),
      'auctionmark_ratio_get_user_info' => array('min' => 0),
      'auctionmark_ratio_new_bid' => array('min' => 0),
      'auctionmark_ratio_new_comment' => array('min' => 0),
      'auctionmark_ratio_new_comment_response' => array('min' => 0),
      'auctionmark_ratio_new_feedback' => array('min' => 0),
      'auctionmark_ratio_new_item' => array('min' => 0),
      'auctionmark_ratio_new_purchase' => array('min' => 0),
      'auctionmark_ratio_update_item' => array('min' => 0),
      'epinions_ratio_get_review_item_id' => array('min' => 0),
      'epinions_ratio_get_reviews_user' => array('min' => 0),
      'epinions_ratio_get_average_rating_trusted_user' => array('min' => 0),
      'epinions_ratio_get_average_rating' => array('min' => 0),
      'epinions_ratio_get_item_reviews_trusted_user' => array('min' => 0),
      'epinions_ratio_update_user_name' => array('min' => 0),
      'epinions_ratio_update_item_title' => array('min' => 0),
      'epinions_ratio_update_review_rating' => array('min' => 0),
      'epinions_ratio_update_trust_rating' => array('min' => 0),
      'epinions_users' => array('min' => 2000),
      'db_driver' => array('required' => TRUE),
      'db_dump' => array('writedir' => TRUE),
      'db_isolation' => array('option' => array('serializable', 'repeatable_read', 'read_committed', 'read_uncommitted'), 'required' => TRUE),
      'db_port' => array('min' => 1000),
      'db_type' => array('option' => array('mysql', 'db2', 'postgres', 'oracle', 'sqlserver', 'sqlite', 'hstore', 'hsqldb', 'h2', 'monetdb', 'nuodb'), 'required' => TRUE),
      'db_url' => array('required' => TRUE),
      'db_user' => array('required' => TRUE),
      'font_size' => array('min' => 6, 'max' => 64),
      'jpab_objects' => array('min' => 100000),
      'jpab_ratio_delete' => array('min' => 0),
      'jpab_ratio_persist' => array('min' => 0),
      'jpab_ratio_retrieve' => array('min' => 0),
      'jpab_ratio_update' => array('min' => 0),
      'jpab_test' => array('option' => array('basic', 'collection', 'inheritance', 'indexing', 'graph')),
      'output' => array('write' => TRUE),
      'resourcestresser_ratio_cpu1' => array('min' => 0),
      'resourcestresser_ratio_cpu2' => array('min' => 0),
      'resourcestresser_ratio_io1' => array('min' => 0),
      'resourcestresser_ratio_io2' => array('min' => 0),
      'resourcestresser_ratio_contention1' => array('min' => 0),
      'resourcestresser_ratio_contention2' => array('min' => 0),
      'seats_customers' => array('min' => 1000),
      'seats_ratio_delete_reservation' => array('min' => 0),
      'seats_ratio_find_flights' => array('min' => 0),
      'seats_ratio_find_open_seats' => array('min' => 0),
      'seats_ratio_new_reservation' => array('min' => 0),
      'seats_ratio_update_customer' => array('min' => 0),
      'seats_ratio_update_reservation' => array('min' => 0),
      'steady_state_threshold' => array('min' => 1, 'max' => 100, 'required' => TRUE),
      'steady_state_window' => array('min' => 1, 'max' => 30, 'required' => TRUE),
      'tatp_ratio_delete_call_forwarding' => array('min' => 0),
      'tatp_ratio_get_access_data' => array('min' => 0),
      'tatp_ratio_get_new_destination' => array('min' => 0),
      'tatp_ratio_get_subscriber_data' => array('min' => 0),
      'tatp_ratio_insert_call_forwarding' => array('min' => 0),
      'tatp_ratio_update_location' => array('min' => 0),
      'tatp_ratio_update_subscriber_data' => array('min' => 0),
      'tatp_subscribers' => array('min' => $this->maxClients),
      'test' => array('option' => array('auctionmark', 'epinions', 'jpab', 'resourcestresser', 'seats', 'tatp', 'tpcc', 'twitter', 'wikipedia', 'ycsb'), 'required' => TRUE),
      'test_clients' => array('min' => 1, 'required' => TRUE),
      'test_clients_step' => array('min' => 1),
      'test_processes' => array('min' => 1, 'required' => TRUE),
      'test_rate' => array('min' => 1, 'required' => TRUE),
      'test_rate_step' => array('min' => 1),
      'test_sample_interval' => array('min' => 1, 'max' => 60, 'required' => TRUE),
      'test_size_ratio' => array('min' => 1, 'max' => 100),
      'test_time' => array('min' => 1, 'required' => TRUE),
      'test_time_step' => array('min' => 1),
      'tpcc_ratio_delivery' => array('min' => 0),
      'tpcc_ratio_new_order' => array('min' => 0),
      'tpcc_ratio_order_status' => array('min' => 0),
      'tpcc_ratio_payment' => array('min' => 0),
      'tpcc_ratio_stock_level' => array('min' => 0),
      'tpcc_warehouses' => array('min' => $this->maxClients),
      'twitter_ratio_get_tweet' => array('min' => 0),
      'twitter_ratio_get_tweet_following' => array('min' => 0),
      'twitter_ratio_get_followers' => array('min' => 0),
      'twitter_ratio_get_user_tweets' => array('min' => 0),
      'twitter_ratio_insert_tweet' => array('min' => 0),
      'twitter_users' => array('min' => 500),
      'wikipedia_pages' => array('min' => 1000),
      'wikipedia_ratio_add_watch_list' => array('min' => 0),
      'wikipedia_ratio_remove_watch_list' => array('min' => 0),
      'wikipedia_ratio_update_page' => array('min' => 0),
      'wikipedia_ratio_get_page_anonymous' => array('min' => 0),
      'wikipedia_ratio_get_page_authenticated' => array('min' => 0),
      'ycsb_ratio_read' => array('min' => 0),
      'ycsb_ratio_insert' => array('min' => 0),
      'ycsb_ratio_scan' => array('min' => 0),
      'ycsb_ratio_update' => array('min' => 0),
      'ycsb_ratio_delete' => array('min' => 0),
      'ycsb_ratio_read_modify_write' => array('min' => 0),
      'ycsb_user_rows' => array('min' => 10000)
    );
    
    $validated = validate_options($options, $validate);
    if (!is_array($validated)) $validated = array();
    
    // validate collectd rrd options
    if (isset($options['collectd_rrd'])) {
      if (!ch_check_sudo()) $validated['collectd_rrd'] = 'sudo privilege is required to use this option';
      else if (!is_dir($options['collectd_rrd_dir'])) $validated['collectd_rrd_dir'] = sprintf('The directory %s does not exist', $options['collectd_rrd_dir']);
      else if ((shell_exec('ps aux | grep collectd | wc -l')*1 < 2)) $validated['collectd_rrd'] = 'collectd is not running';
      else if ((shell_exec(sprintf('find %s -maxdepth 1 -type d 2>/dev/null | wc -l', $options['collectd_rrd_dir']))*1 < 2)) $validated['collectd_rrd_dir'] = sprintf('The directory %s is empty', $options['collectd_rrd_dir']);
    }
    
    // ratios for each test must sum to 100
    foreach($options['test'] as $test) {
      if (($sum = array_sum($this->getTestWeights($test))) != 100) {
        $validated['test'] = sprintf('The sum of ratios for test %s must be 100 (currently %d)', $test, $sum);
        break;
      }
    }
    
    if (!isset($validate['db_url']) && !preg_match('/^[a-zA-Z0-9]+:[a-zA-Z]+:\/\/[a-zA-Z0-9\.]+[\:0-9]*\/[a-zA-Z0-9]+$/', $options['db_url'])) $validated['db_url'] = sprintf('--db_url %s is not valid. Correct format for a JDBC URL is jdbc:[db_type]://[db_host]:[db_port]/[db_name]', $options['db_url']);
      
    // OLTP-Bench is not built yet
    if (!OltpBenchTest::oltpBenchIsBuilt()) {
      print_msg('OLTP-Bench has not been built yet. Attempting to build using ant', $this->verbose, __FILE__, __LINE__);
      $buffer = shell_exec(sprintf('cd %s && ant', dirname(__FILE__) . '/oltpbench'));
      if (strpos($buffer, 'oracle.sql.TIMESTAMP')) {
        print_msg('Build Failed - attempting re-build with oracle.sql.TIMESTAMP reference removed from SQLUtil.java', $this->verbose, __FILE__, __LINE__, TRUE);
        unlink($f = dirname(__FILE__) . '/oltpbench/src/com/oltpbenchmark/util/SQLUtil.java');
        exec(sprintf('cp %s %s', dirname(__FILE__) . '/SQLUtil-patched.java', $f));
        passthru(sprintf('cd %s && ant', dirname(__FILE__) . '/oltpbench'));
      }
      if (OltpBenchTest::oltpBenchIsBuilt()) print_msg('OLTP-Bench built successfully', $this->verbose, __FILE__, __LINE__);
      else {
        print_msg('Unable to build OLTP-Bench', $this->verbose, __FILE__, __LINE__, TRUE);
        $validated['test'] = 'Benchmark cannot be run because OLTP-Bench is not built';
      }
    }
    
    // JPAB test only compatible with MySQL, DB2, PostgreSQL, Oracle and SQL Server
    if (in_array('jpab', $options['test']) && !in_array($options['db_type'], array('mysql', 'db2', 'postgres', 'oracle', 'sqlserver'))) $validated['test'] = 'JPAB Benchmark is only compatible with MySQL, DB2, PostgreSQL, Oracle and SQL Server';
    
    // db dump file
    if (isset($options['db_dump']) && is_dir($options['db_dump'])) $validated['db_dump'] = '--db_dump cannot be a directory';
    
    // test size
    if (isset($options['test_size']) && 
        !(preg_match('/^\//', trim($this->options['test_size'])) ? get_free_space($this->options['test_size']) : size_from_string($this->options['test_size']))) {
      $validated['test_size'] = sprintf('%s is not a valid size string (e.g. 100 GB), directory or device', $options['test_size']);
    }
    
    return $validated;
  }
  
  
  /**
   * returns TRUE if wkhtmltopdf is installed, FALSE otherwise
   * @return boolean
   */
  public final static function wkhtmltopdfInstalled() {
    $ecode = trim(exec('which wkhtmltopdf; echo $?'));
    return $ecode == 0;
  }
}
?>
