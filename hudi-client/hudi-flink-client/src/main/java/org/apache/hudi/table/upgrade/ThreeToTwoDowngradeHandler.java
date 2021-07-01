package org.apache.hudi.table.upgrade;

import org.apache.hudi.common.config.ConfigProperty;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.metadata.HoodieTableMetadataWriter;

import java.util.Collections;
import java.util.Map;

/**
 * Downgrade handle to assist in downgrading hoodie table from version 3 to 2.
 */
public class ThreeToTwoDowngradeHandler implements DowngradeHandler {

  @Override
  public Map<ConfigProperty, String> downgrade(HoodieWriteConfig config, HoodieEngineContext context, String instantTime) {
    if (config.useFileListingMetadata()) {
      // Metadata Table in version 2 is synchronous and in version 1 is asynchronous. Downgrading to synchronous
      // removes the checks in code to decide whether to use a LogBlock or not. Also, the schema for the
      // table has been updated and is not forward compatible. Hence, we need to delete the table.
      HoodieTableMetadataWriter.removeMetadataTable(config.getBasePath(), context);
    }
    return Collections.emptyMap();
  }
}