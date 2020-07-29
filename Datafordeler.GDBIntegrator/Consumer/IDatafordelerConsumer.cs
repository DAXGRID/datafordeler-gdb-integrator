using System;

namespace Datafordeler.DBIntegrator.Consumer
{
    public interface IDatafordelerConsumer : IDisposable
    {
        void Start();
    }
}