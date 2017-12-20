package org.apache.drill.exec.store.druid;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import org.apache.drill.common.logical.StoragePluginConfig;
import org.apache.drill.common.logical.StoragePluginConfigBase;

@JsonTypeName(DruidStoragePluginConfig.NAME)
public class DruidStoragePluginConfig extends StoragePluginConfigBase {

    static final org.slf4j.Logger logger = org.slf4j.LoggerFactory
            .getLogger(DruidStoragePluginConfig.class);

    public static final String NAME = "druid";

    @JsonProperty
    private final String brokerAddress;

    @JsonProperty
    private final String coordinatorAddress;

    @JsonCreator
    public DruidStoragePluginConfig(
            @JsonProperty("brokerAddress") String brokerAddress,
            @JsonProperty("coordinatorAddress") String coordinatorAddress) {

        this.brokerAddress = brokerAddress;
        this.coordinatorAddress = coordinatorAddress;
        //TODO Make this configurable.
    }

    @Override
    public boolean equals(Object that) {
        if (this == that) {
            return true;
        } else if (that == null || getClass() != that.getClass()) {
            return false;
        }
        DruidStoragePluginConfig thatConfig = (DruidStoragePluginConfig) that;
        return
                (this.brokerAddress.equals(thatConfig.brokerAddress)
                        && this.coordinatorAddress.equals(thatConfig.coordinatorAddress));
    }

    @Override
    public int hashCode()
    {
        int brokerAddressHashCode = this.brokerAddress != null ? this.brokerAddress.hashCode() : 0;
        int coordinatorAddressHashCode = this.coordinatorAddress != null ? this.coordinatorAddress.hashCode() : 0;
        return brokerAddressHashCode ^ coordinatorAddressHashCode;
    }

    public String GetCoordinatorURI() {
        return this.coordinatorAddress;
    }

    public String GetBrokerURI() {
        return this.brokerAddress;
    }
}
