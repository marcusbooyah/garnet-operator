<p style="text-align:center;" align="center">
  <img src="https://raw.githubusercontent.com/marcusbooyah/garnet-operator/main/logo.png" width="175" height="175"/></a>  
</p>

⚠️ UNDER ACTIVE DEVELOPMENT

# Garnet Operator for Kubernetes


## Introduction
The Garnet Operator for Kubernetes is an operator for managing Garnet Clusters in a Kubernetes Cluster. 

## Installation
The included helm chart contains the necessary permissions and configuration to run the operator in a Kubernetes cluster.

To install the operator, run the following command:
```sh
$> helm install garnet-operator ./charts/garnet-operator
```

Then deploy a GarnetCluster resource:
```yaml
apiVersion: garnet.k8soperator.io/v1alpha1
kind: GarnetCluster
metadata:
  name: test-cluster
  namespace: garnet
spec:
  numberOfPrimaries: 1
  replicationFactor: 1
```
