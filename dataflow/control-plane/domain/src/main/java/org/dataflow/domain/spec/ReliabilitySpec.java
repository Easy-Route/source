package org.dataflow.domain.spec;

public record ReliabilitySpec(DeliveryGuarantee deliveryGuarantee) {
    public ReliabilitySpec {
        if (deliveryGuarantee == null) {
            deliveryGuarantee = DeliveryGuarantee.EFFECTIVELY_ONCE;
        }
    }

    public enum DeliveryGuarantee {
        AT_LEAST_ONCE,
        EFFECTIVELY_ONCE
    }
}
