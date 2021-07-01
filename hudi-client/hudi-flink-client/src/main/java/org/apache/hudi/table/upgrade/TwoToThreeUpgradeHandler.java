package org.apache.hudi.table.upgrade;

import org.apache.hudi.common.config.ConfigProperty;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.metadata.HoodieTableMetadataWriter;

import java.util.Collections;
import java.util.Map;

public class TwoToThreeUpgradeHandler implements UpgradeHandler {
  @Override
  public Map<ConfigProperty, String> upgrade(HoodieWriteConfig config, HoodieEngineContext context, String instantTime) {
    if (config.useFileListingMetadata()) {
      // Metadata Table in version 1 is asynchronous and in version 2 is synchronous. Synchronous table will not
      // sync any instants not already synced. So its simpler to re-bootstrap the table. Also, the schema for the
      // table has been updated and is not backward compatible.
      HoodieTableMetadataWriter.removeMetadataTable(config.getBasePath(), context);
    }
    return Collections.emptyMap();
  }
}