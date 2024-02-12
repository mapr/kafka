package org.apache.kafka.connect.runtime.rest.util;

import org.apache.commons.io.FileUtils;
import org.apache.kafka.connect.runtime.rest.RestServerConfig;

import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletResponse;
import java.io.File;
import java.io.IOException;
import java.util.Properties;

public class HeadersFilter  implements Filter {

  private Properties headerProperties = new Properties();

  @Override
  public void init(FilterConfig filterConfig) throws ServletException {
    String headersFilename = filterConfig.getInitParameter(RestServerConfig.HEADERS_FILE_CONFIG);
    File headerFile = new File(headersFilename);
    try {
      headerProperties.loadFromXML(FileUtils.openInputStream(headerFile));
    } catch (IOException e) {
      throw new ServletException(e);
    }
  }

  @Override
  public void doFilter(ServletRequest servletRequest, ServletResponse servletResponse, FilterChain filterChain) throws IOException, ServletException {
    HttpServletResponse httpServletResponse = (HttpServletResponse) servletResponse;
    headerProperties.forEach( (k, v) -> {
      httpServletResponse.addHeader((String) k, v.toString());
    });

    // pass the request along the filter chain
    filterChain.doFilter(servletRequest, servletResponse);
  }

  @Override
  public void destroy() { }
}
