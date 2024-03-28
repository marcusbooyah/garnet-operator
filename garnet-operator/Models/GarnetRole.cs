namespace GarnetOperator.Models
{
    public enum GarnetRole
    {
        Primary = 0,
        Replica,
        Handshake,
        None,
        Leaving
    }
}
