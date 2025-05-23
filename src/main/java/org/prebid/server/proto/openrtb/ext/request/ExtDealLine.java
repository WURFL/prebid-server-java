package org.prebid.server.proto.openrtb.ext.request;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.iab.openrtb.request.Format;
import lombok.Value;

import java.util.List;

@Value(staticConstructor = "of")
public class ExtDealLine {

    @JsonProperty("lineitemid")
    String lineItemId;

    @JsonProperty("extlineitemid")
    String extLineItemId;

    List<Format> sizes;

    /**
     * Used to distinguish which deal belongs to bidder
     * <p>
     * Note: should not be sent to bidder exchange!
     */
    String bidder;
}
