using System;

namespace SupplyCollectorTestHarness
{
    public class AssemblyMissingException : Exception
    {
        public AssemblyMissingException() {
        }

        public AssemblyMissingException(string message) : base(message) {
        }

        public AssemblyMissingException(string message, Exception innerException) : base(message, innerException) {
        }
    }
}
