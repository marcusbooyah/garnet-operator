namespace GarnetOperator
{
    public static class Constants
    {
        public const string OperatorName = "garnet-operator";
        public const string KubernetesGroup = "garnet.k8soperator.io";
        public const string GarnetContainer = "garnet-node";
        public static class Conditions
        {
            public const string ClusterOk = "ClusterOk";
            public const string Scaling = "Scaling";
            public const string Rebalancing = "Rebalancing";
            public const string RollingUpdate = "RollingUpdate";
            public const string StatusTrue = "True";
            public const string StatusFalse = "False";
            public const string ScalingUpMessage = "cluster needs more pods";
            public const string ScalingUpReason = "cluster needs more pods";
            public const string ScalingDownMessage = "cluster needs less pods";
            public const string ScalingDownReason = "cluster needs less pods";
            public const string RebalancingMessage = "cluster tolology has changed";
            public const string RebalancingReason = "cluster topology has changed";
        }

        public static class ClusterStatus
        {
            public const string Ok = "Okay";
            public const string NotOkay = "NotOkay";
            public const string Scaling = "Scaling";
            public const string Rebalancing = "Rebalancing";
            public const string RollingUpdate = "RollingUpdate";
        }

        public static class Ports
        {
            public const int Redis = 6379;
            public const string RedisName = "redis";
        }

        public static class Labels
        {
            public const string ManagedByKey = "app.kubernetes.io/managed-by";
            public const string ClusterId = $"{Constants.KubernetesGroup}/cluster-id";
        }

        public static class Annotations
        {
            public const string NodeRole = $"{Constants.KubernetesGroup}/role";
        }
    }
}
