using System.ComponentModel;

namespace GarnetOperator.Models
{
    /// <summary>
    /// Options for defining the container image.
    /// </summary>
    public class ImageSpec
    {
        /// <summary>
        /// The image repository.
        /// </summary>
        [DefaultValue("ghcr.io/marcusbooyah/garnet-server")]
        public string Repository { get; set; } = "ghcr.io/marcusbooyah/garnet-server";

        /// <summary>
        /// The image pull policy.
        /// </summary>
        [DefaultValue("IfNotPresent")]
        public string PullPolicy { get; set; } = "IfNotPresent";

        /// <summary>
        /// The image tag.
        /// </summary>
        [DefaultValue("latest")]
        public string Tag { get; set; } = "latest";

        public override string ToString()
        {
            return Repository + ":" + Tag;
        }
    }
}
