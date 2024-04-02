namespace GarnetOperator
{
    /// <summary>
    /// Constants used in the Garnet Operator.
    /// </summary>
    public static class Constants
    {
        /// <summary>
        /// The name of the Garnet Operator.
        /// </summary>
        public const string OperatorName = "garnet-operator";

        /// <summary>
        /// The Kubernetes group for the Garnet Operator.
        /// </summary>
        public const string KubernetesGroup = "garnet.k8soperator.io";

        /// <summary>
        /// The name of the Garnet container.
        /// </summary>
        public const string GarnetContainer = "garnet-node";

        /// <summary>
        /// The version of the Garnet Operator.
        /// </summary>
        public const string Version = "0.0.1";

        /// <summary>
        /// Constants related to conditions.
        /// </summary>
        public static class Conditions
        {
            /// <summary>
            /// The condition for cluster being okay.
            /// </summary>
            public const string ClusterOk = "ClusterOk";

            /// <summary>
            /// The condition for scaling.
            /// </summary>
            public const string Scaling = "Scaling";

            /// <summary>
            /// The condition for rebalancing.
            /// </summary>
            public const string Rebalancing = "Rebalancing";

            /// <summary>
            /// The condition for rolling update.
            /// </summary>
            public const string RollingUpdate = "RollingUpdate";

            /// <summary>
            /// Cluster is being initialized.
            /// </summary>
            public const string Initializing = "Initializing";

            /// <summary>
            /// The status value for true.
            /// </summary>
            public const string StatusTrue = "True";

            /// <summary>
            /// The status value for false.
            /// </summary>
            public const string StatusFalse = "False";

            /// <summary>
            /// The message for scaling up.
            /// </summary>
            public const string ScalingUpMessage = "cluster needs more pods";

            /// <summary>
            /// The reason for scaling up.
            /// </summary>
            public const string ScalingUpReason = "cluster needs more pods";

            /// <summary>
            /// The message for scaling down.
            /// </summary>
            public const string ScalingDownMessage = "cluster needs less pods";

            /// <summary>
            /// The reason for scaling down.
            /// </summary>
            public const string ScalingDownReason = "cluster needs less pods";

            /// <summary>
            /// The message for rebalancing.
            /// </summary>
            public const string RebalancingMessage = "cluster topology has changed";

            /// <summary>
            /// The reason for rebalancing.
            /// </summary>
            public const string RebalancingReason = "cluster topology has changed";
        }

        /// <summary>
        /// Constants related to cluster status.
        /// </summary>
        public static class ClusterStatus
        {
            /// <summary>
            /// The status value for cluster being okay.
            /// </summary>
            public const string Ok = "Okay";

            /// <summary>
            /// The status value for cluster not being okay.
            /// </summary>
            public const string NotOkay = "NotOkay";

            /// <summary>
            /// The status value for scaling.
            /// </summary>
            public const string Scaling = "Scaling";

            /// <summary>
            /// The status value for rebalancing.
            /// </summary>
            public const string Rebalancing = "Rebalancing";

            /// <summary>
            /// The status value for rolling update.
            /// </summary>
            public const string RollingUpdate = "RollingUpdate";
        }

        /// <summary>
        /// Constants related to ports.
        /// </summary>
        public static class Ports
        {
            /// <summary>
            /// The Redis port number.
            /// </summary>
            public const int Redis = 6379;

            /// <summary>
            /// The name of the Redis port.
            /// </summary>
            public const string RedisName = "redis";
        }

        /// <summary>
        /// Constants related to labels.
        /// </summary>
        public static class Labels
        {
            /// <summary>
            /// The label for managed by key.
            /// </summary>
            public const string ManagedByKey = "app.kubernetes.io/managed-by";

            /// <summary>
            /// The label for cluster ID.
            /// </summary>
            public const string ClusterId = $"{Constants.KubernetesGroup}/cluster-id";
        }

        /// <summary>
        /// Constants related to annotations.
        /// </summary>
        public static class Annotations
        {
            /// <summary>
            /// The annotation for node role.
            /// </summary>
            public const string NodeRole = $"{Constants.KubernetesGroup}/role";
        }

        /// <summary>
        /// Constants related to OLM (Operator Lifecycle Manager).
        /// </summary>
        public static class Olm
        {
            /// <summary>
            /// The description of the Garnet cluster.
            /// </summary>
            public const string GarnetClusterDescription = "";

            /// <summary>
            /// An example of the Garnet cluster.
            /// </summary>
            public const string GarnetClusterExample = "";

            /// <summary>
            /// The full description of the Garnet Operator.
            /// </summary>
            public const string FullDescription = "";

            /// <summary>
            /// The short description of the Garnet Operator.
            /// </summary>
            public const string ShortDescription = "";

            /// <summary>
            /// The minimum Kubernetes version required by the Garnet Operator.
            /// </summary>
            public const string MinKubeVersion = "";

            /// <summary>
            /// The maturity level of the Garnet Operator.
            /// </summary>
            public const string Maturity = "";

            /// <summary>
            /// The default channel for the Garnet Operator.
            /// </summary>
            public const string DefaultChannel = "";
        }

        public class Redis
        {
            public const int Slots = 16384;
        }
    }
}
