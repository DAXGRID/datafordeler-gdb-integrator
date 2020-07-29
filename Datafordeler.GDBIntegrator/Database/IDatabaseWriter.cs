using System;
using System.Collections.Generic;
using System.Text;

namespace Datafordeler.GDBIntegrator.Database
{
    public interface IDatabaseWriter
    {
        void UpsertStuff(string data);
    }
}
