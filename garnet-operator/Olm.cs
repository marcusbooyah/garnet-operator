using GarnetOperator;
using GarnetOperator.Models;

using Neon.Operator.Attributes;
using Neon.Operator.OperatorLifecycleManager;

[assembly: Name(Name = "garnet-operator")]
[assembly: DisplayName(DisplayName = "Garnet Operator")]
[assembly: Version(Constants.Version)]
[assembly: Maturity(Constants.Olm.Maturity)]
[assembly: MinKubeVersion(Constants.Olm.MinKubeVersion)]
[assembly: Keyword("garnet", "redis", "cache")]
[assembly: DefaultChannel(Constants.Olm.DefaultChannel)]
[assembly: OwnedEntity<V1alpha1GarnetCluster>(
    Description = Constants.Olm.GarnetClusterDescription,
    DisplayName = "GarnetCluster",
    ExampleYaml = Constants.Olm.GarnetClusterExample)]
[assembly: Description(
    FullDescription  = Constants.Olm.FullDescription,
    ShortDescription = Constants.Olm.ShortDescription)]
[assembly: Provider(
    Name = "marcusbooyah",
    Url  = "https://github.com/marcusbooyah/garnet-operator")]
[assembly: Maintainer(
    Name     = "Marcus Bowyer",
    Email    = "marcus@bowyer.me",
    GitHub   = "marcusbooyah",
    Reviewer = true)]
[assembly: Category(
    Category = Category.Database)]
[assembly: Capabilities(
    Capability = CapabilityLevel.FullLifecycle)]
[assembly: ContainerImage(
    Repository = "ghcr.io/marcusbooyah/garnet-operator",
    Tag = Constants.Version)]
[assembly: Repository(
    Repository = "https://github.com/marcusbooyah/garnet-operator")]
[assembly: Link(
    Name = "GitHub",
    Url = "https://github.com/marcusbooyah/garnet-operator")]
[assembly: InstallMode(
    Type = InstallModeType.OwnNamespace
    | InstallModeType.SingleNamespace
    | InstallModeType.MultiNamespace
    | InstallModeType.AllNamespaces)]