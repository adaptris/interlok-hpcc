package com.adaptris.hpcc;

import org.apache.commons.exec.LogOutputStream;
import org.apache.oro.text.GlobCompiler;
import org.apache.oro.text.regex.Pattern;
import org.apache.oro.text.regex.Perl5Compiler;
import org.apache.oro.text.regex.Perl5Matcher;

// C:\adaptris\HPCCSystems\5.4.2\clienttools\bin>dfuplus action=list server=http://192.168.56.101:8011 name="zzlc*"
// List zzlc*
// 00000000 2016-06-16 15:55:40 18852 20308 "ERROR: Error connecting to 192.168.56.101:8011"
// 00000001 2016-06-16 15:55:40 18852 20308 "-3: connection failed Target: T>192.168.56.101, Raised in:
// ..\..\..\HPCC-Platform\system\jlib\jsocket.cpp, line 1250"
// SOAP Connection error
//
// C:\adaptris\HPCCSystems\5.4.2\clienttools\bin>dfuplus action=list server=http://192.168.56.101:8010
// name="zzlc::???::farm_data_rel"
// List zzlc::???::farm_data_rel
// zzlc::csv::farm_data_rel
//
// C:\adaptris\HPCCSystems\5.4.2\clienttools\bin>dfuplus action=list server=http://192.168.56.101:8010
// name="zzlc::???::farm_data_rel"
// List zzlc::???::farm_data_rel
// zzlc::csv::farm_data_rel
//
// C:\adaptris\HPCCSystems\5.4.2\clienttools\bin>dfuplus action=list server=http://192.168.56.101:8010 name="*zzlc*"
// List *zzlc*
// myzzlc::csv::farm_data_rel
// zzlc::csv::farm_data_rel
public class ListOutputParser extends LogOutputStream {

  private static final String FIRST_LINE = "List %s";
  private static final String INTERNAL_LOGGING_REGEXP = "^[0-9]+ [0-9\\-]+ [0-9:]+ [0-9]+ [0-9]+ \"(.*)\"";
  private transient boolean hasErrors = false;
  private transient boolean ready = false;
  private transient Pattern globPattern, errorPattern;
  private transient Perl5Matcher matcher;
  private transient boolean found = false;
  private transient String expected;

  public ListOutputParser(String filespec) throws Exception {
    super();
    globPattern = new GlobCompiler().compile(filespec);
    errorPattern = new Perl5Compiler().compile(INTERNAL_LOGGING_REGEXP);
    matcher = new Perl5Matcher();
    expected = String.format(FIRST_LINE, filespec);
  }

  @Override
  protected void processLine(String line, int logLevel) {
    checkHasErrors(line);
    if (expected.equalsIgnoreCase(line)) {
      ready = true;
      return;
    }
    if (ready) {
      checkIsMatch(line);
    }
  }

  private void checkIsMatch(String line) {
    if (matcher.matches(line, globPattern)) {
      found = true;
    }
  }

  private void checkHasErrors(String line) {
    if (matcher.matches(line, errorPattern)) {
      hasErrors = true;
    }
  }
  public boolean hasErrors() {
    return hasErrors;
  }

  public boolean found() {
    return found;
  }
}
