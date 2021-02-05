using MongoDB.Driver;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace JohnKnoop.MongoRepository
{
    internal static class SessionContainer
    {
        private static AsyncLocal<IClientSessionHandle> _ambientSession = new AsyncLocal<IClientSessionHandle>();
        internal static IClientSessionHandle AmbientSession => _ambientSession?.Value;
        internal static void SetSession(IClientSessionHandle session)
		{
            _ambientSession.Value = session;
		}

        internal static ConcurrentDictionary<string, IClientSessionHandle> SessionsByTransactionIdentifier = new ConcurrentDictionary<string, IClientSessionHandle>();
    }
}
