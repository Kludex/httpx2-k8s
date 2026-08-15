# Kubernetes API coverage

This page is generated from the pinned official Kubernetes OpenAPI manifests.

| Kubernetes | Official client | Operations | Schemas | Gaps | APIs not in version |
| --- | --- | ---: | ---: | ---: | ---: |
| v1.31.0 | 31.0.0 | 944/944 | 635/635 | 0 | 9 |
| v1.32.0 | 32.0.1 | 947/947 | 638/638 | 0 | 10 |
| v1.33.0 | 33.1.0 | 1041/1041 | 707/707 | 0 | 8 |
| v1.34.0 | 34.1.0 | 1062/1062 | 730/730 | 0 | 7 |
| v1.35.0 | 35.0.0 | 1067/1067 | 735/735 | 0 | 7 |
| v1.36.0 | 36.0.3 | 1123/1123 | 771/771 | 0 | 7 |

Temporary exceptions: **0**.

Consolidated Pythonic compatibility names: **471**.

Pythonic compatibility methods may consolidate generated names, but every underlying HTTP operation remains available through the exact typed method recorded in the JSON report.

## APIs not present in each Kubernetes version

- **v1.31.0**: certificates.k8s.io/v1beta1, coordination.k8s.io/v1alpha2, coordination.k8s.io/v1beta1, resource.k8s.io/v1, resource.k8s.io/v1beta1, resource.k8s.io/v1beta2, scheduling.k8s.io/v1alpha1, scheduling.k8s.io/v1alpha2, storagemigration.k8s.io/v1beta1
- **v1.32.0**: authentication.k8s.io/v1alpha1, certificates.k8s.io/v1beta1, coordination.k8s.io/v1alpha1, coordination.k8s.io/v1beta1, flowcontrol.apiserver.k8s.io/v1beta3, resource.k8s.io/v1, resource.k8s.io/v1beta2, scheduling.k8s.io/v1alpha1, scheduling.k8s.io/v1alpha2, storagemigration.k8s.io/v1beta1
- **v1.33.0**: authentication.k8s.io/v1alpha1, authentication.k8s.io/v1beta1, coordination.k8s.io/v1alpha1, flowcontrol.apiserver.k8s.io/v1beta3, resource.k8s.io/v1, scheduling.k8s.io/v1alpha1, scheduling.k8s.io/v1alpha2, storagemigration.k8s.io/v1beta1
- **v1.34.0**: authentication.k8s.io/v1alpha1, authentication.k8s.io/v1beta1, coordination.k8s.io/v1alpha1, flowcontrol.apiserver.k8s.io/v1beta3, scheduling.k8s.io/v1alpha1, scheduling.k8s.io/v1alpha2, storagemigration.k8s.io/v1beta1
- **v1.35.0**: authentication.k8s.io/v1alpha1, authentication.k8s.io/v1beta1, coordination.k8s.io/v1alpha1, flowcontrol.apiserver.k8s.io/v1beta3, scheduling.k8s.io/v1alpha2, storage.k8s.io/v1alpha1, storagemigration.k8s.io/v1alpha1
- **v1.36.0**: authentication.k8s.io/v1alpha1, authentication.k8s.io/v1beta1, coordination.k8s.io/v1alpha1, flowcontrol.apiserver.k8s.io/v1beta3, scheduling.k8s.io/v1alpha1, storage.k8s.io/v1alpha1, storagemigration.k8s.io/v1alpha1

The machine-readable report is [`compliance/report.json`](../compliance/report.json).
