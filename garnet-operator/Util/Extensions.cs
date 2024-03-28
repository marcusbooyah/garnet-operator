using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

using GarnetOperator.Models;

using k8s;
using k8s.Models;

using Neon.K8s;
using Neon.K8s.Core;
using Neon.Operator.Util;

namespace GarnetOperator
{
    /// <summary>
    /// Provides extension methods for various types.
    /// </summary>
    public static class Extensions
    {
        /// <summary>
        /// Adds the key-value pairs from the specified collection to the source dictionary.
        /// </summary>
        /// <typeparam name="T">The type of the dictionary keys.</typeparam>
        /// <typeparam name="S">The type of the dictionary values.</typeparam>
        /// <param name="source">The source dictionary.</param>
        /// <param name="collection">The collection containing the key-value pairs to add.</param>
        public static void AddRange<T, S>(this IDictionary<T, S> source, IDictionary<T, S> collection)
        {
            if (collection == null)
            {
                throw new ArgumentNullException(nameof(collection), "Collection is null");
            }

            foreach (var item in collection)
            {
                source[item.Key] = item.Value;
            }
        }

        /// <summary>
        /// Compares two objects by serializing them to JSON and checking for equality.
        /// </summary>
        /// <param name="x">The first object to compare.</param>
        /// <param name="y">The second object to compare.</param>
        /// <returns><c>true</c> if the objects are equal; otherwise, <c>false</c>.</returns>
        public static bool JsonEquals(this object x, object y)
        {
            return KubernetesHelper.JsonSerialize(x) == KubernetesHelper.JsonSerialize(y);
        }

        /// <summary>
        /// Sets the owner reference for a list of owner references.
        /// </summary>
        /// <param name="ownerReferences">The list of owner references.</param>
        /// <param name="reference">The owner reference to set.</param>
        /// <returns><c>true</c> if the owner reference was added or updated; otherwise, <c>false</c>.</returns>
        public static bool SetOwnerReference(this IList<V1OwnerReference> ownerReferences, V1OwnerReference reference)
        {
            if (!ownerReferences.Any(o => o.Uid == reference.Uid))
            {
                ownerReferences.Add(reference);
                return true;
            }

            var existingRef = ownerReferences.Where(o => o.Uid == reference.Uid).FirstOrDefault();

            if (!existingRef.JsonEquals(reference))
            {
                ownerReferences = ownerReferences.Where(o => o.Uid != reference.Uid).ToList();
                ownerReferences.Add(reference);
                return true;
            }

            return false;
        }

        /// <summary>
        /// Converts a dictionary of labels to a label selector string.
        /// </summary>
        /// <param name="labels">The dictionary of labels.</param>
        /// <returns>The label selector string.</returns>
        public static string ToLabelSelector(this Dictionary<string, string> labels)
        {
            if (labels.Count == 0)
            {
                return string.Empty;
            }

            var sb = new StringBuilder();

            var index = 0;
            foreach (var label in labels)
            {
                sb.Append(label.Key + "=" + label.Value);
                if (index < labels.Count - 1)
                {
                    sb.Append(",");
                }
                index++;
            }

            return sb.ToString();
        }

        public static async Task SetConditionAsync(this V1alpha1GarnetCluster resource,
            IKubernetes k8s,
            string type,
            string status,
            string reason = null,
            string message = null)
        {
            var condition = new V1Condition()
            {
                LastTransitionTime = DateTime.UtcNow,
                Message            = message,
                Reason             = reason,
                Status             = status,
                Type               = type,
            };

            var patch = OperatorHelper.CreatePatch<V1alpha1GarnetCluster>();

            if (resource.Status == null)
            {
                resource.Status = new V1alpha1GarnetClusterStatus();
                patch.Replace(r => r.Status, resource.Status);
            }

            resource.Status.Conditions ??= new List<V1Condition>();

            if (!resource.Status.Conditions.Any(c => c.Type == condition.Type))
            {
                resource.Status.Conditions.Add(condition);
            }
            else
            {
                resource.Status.Conditions = resource.Status.Conditions.Where(c => c.Type != condition.Type).ToList();
                resource.Status.Conditions.Add(condition);
            }

            patch.Replace(r => r.Status.Conditions, resource.Status.Conditions);

            await k8s.CustomObjects.PatchNamespacedCustomObjectStatusAsync<V1alpha1GarnetCluster>(
                patch: OperatorHelper.ToV1Patch(patch),
                name: resource.Name(),
                namespaceParameter: resource.Namespace());
        }
        public static async Task AddNodeAsync(this V1alpha1GarnetCluster resource,
            IKubernetes k8s,
            GarnetNode node)
        {
            var patch = OperatorHelper.CreatePatch<V1alpha1GarnetCluster>();

            if (resource.Status == null)
            {
                resource.Status = new V1alpha1GarnetClusterStatus();
                patch.Replace(r => r.Status, resource.Status);
            }

            if (resource.Status.Cluster == null)
            {
                resource.Status.Cluster = new Cluster();
                patch.Replace(r => r.Status.Cluster, resource.Status.Cluster);

            }

            resource.Status.Cluster.Nodes.Add(node);


            patch.Add(r => r.Status.Cluster.Nodes, node);

            await k8s.CustomObjects.PatchNamespacedCustomObjectStatusAsync<V1alpha1GarnetCluster>(
                patch: OperatorHelper.ToV1Patch(patch),
                name: resource.Name(),
                namespaceParameter: resource.Namespace());
        }
        public static async Task RemoveNodeAsync(this V1alpha1GarnetCluster resource,
            IKubernetes k8s,
            GarnetNode node)
        {
            var patch = OperatorHelper.CreatePatch<V1alpha1GarnetCluster>();

            var remove = resource.Status.Cluster.Nodes.Where(n => n.Id == node.Id).FirstOrDefault();

            if (remove == null)
            {
                return;
            }

            resource.Status.Cluster.Nodes.Remove(remove);

            patch.Replace(r => r.Status.Cluster.Nodes, resource.Status.Cluster.Nodes);

            await k8s.CustomObjects.PatchNamespacedCustomObjectStatusAsync<V1alpha1GarnetCluster>(
                patch: OperatorHelper.ToV1Patch(patch),
                name: resource.Name(),
                namespaceParameter: resource.Namespace());
        }
    }
}