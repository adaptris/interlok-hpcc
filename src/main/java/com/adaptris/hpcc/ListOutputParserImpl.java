package com.adaptris.hpcc;

import org.apache.commons.exec.LogOutputStream;
import org.apache.oro.text.regex.Pattern;
import org.apache.oro.text.regex.Perl5Compiler;
import org.apache.oro.text.regex.Perl5Matcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

abstract class ListOutputParserImpl extends LogOutputStream {

  protected transient Logger log = LoggerFactory.getLogger("com.adaptris.hpcc.OutputParser");
  private static final String FIRST_LINE = "List %s";
  private static final String INTERNAL_LOGGING_REGEXP = "^[0-9]+ [0-9\\-]+ [0-9:]+ [0-9]+ [0-9]+ \"(.*)\"";
  private transient boolean hasErrors = false;
  private transient Pattern errorPattern;
  protected transient Perl5Matcher matcher;
  protected transient String expectedFirstLine;

  public ListOutputParserImpl(String filespec) throws Exception {
    super();
    errorPattern = new Perl5Compiler().compile(INTERNAL_LOGGING_REGEXP);
    matcher = new Perl5Matcher();
    expectedFirstLine = String.format(FIRST_LINE, filespec);
  }

  protected void checkHasErrors(String line) {
    if (matcher.matches(line, errorPattern)) {
      hasErrors = true;
    }
  }

  public boolean hasErrors() {
    return hasErrors;
  }
}
