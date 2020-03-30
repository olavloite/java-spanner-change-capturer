package com.google.cloud.spanner.capturer;

import com.google.cloud.spanner.ForwardingStructReader;
import com.google.cloud.spanner.ResultSet;
import com.google.cloud.spanner.Struct;
import com.google.cloud.spanner.capturer.SpannerTableChangeCapturer.Row;

class RowImpl extends ForwardingStructReader implements Row {
  private final ResultSet delegate;

  RowImpl(ResultSet delegate) {
    super(delegate);
    this.delegate = delegate;
  }

  @Override
  public Struct asStruct() {
    return delegate.getCurrentRowAsStruct();
  }
}
