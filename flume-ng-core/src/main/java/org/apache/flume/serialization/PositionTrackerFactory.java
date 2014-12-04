package org.apache.flume.serialization;

import java.io.File;

import org.apache.flume.FlumeException;
import org.apache.flume.annotations.InterfaceAudience;
import org.apache.flume.annotations.InterfaceStability;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@InterfaceAudience.Private
@InterfaceStability.Unstable
public final class PositionTrackerFactory {

  private static final Logger log = LoggerFactory.getLogger(PositionTrackerFactory.class);

  private final Class<?> positionTrackerClass;
  private final File trackerDir;

  private PositionTrackerFactory() {
    throw new UnsupportedOperationException("private constructor");
  }

  private PositionTrackerFactory(final Class<?> positionTrackerClass, final File trackerDir) {
    this.positionTrackerClass = positionTrackerClass;
    this.trackerDir = trackerDir;
    initializeTrackerDir();
  }



  public static PositionTrackerFactory getInstance() {
    return new PositionTrackerFactory(, new File(trackerDir));
  }


  public static PositionTrackerFactory getInstance(final String trackerDir) {
    return new PositionTrackerFactory(new File(trackerDir));
  }

}
