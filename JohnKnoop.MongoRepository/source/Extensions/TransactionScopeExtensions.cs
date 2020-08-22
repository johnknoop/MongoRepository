using MongoDB.Driver;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Transactions;

namespace JohnKnoop.MongoRepository.Extensions
{
	public static class TransactionScopeExtensions
	{
		public static async Task<TransactionScope> RetryAsync(this TransactionScope transaction, Func<TransactionScope, Task> transactionBody, int maxRetries = default)
		{
			var tries = 0;

			while (true)
			{
				try
				{
					await transactionBody(transaction);
					return transaction;
				}
				catch (MongoException ex) when (ex.HasErrorLabel("TransientTransactionError"))
				{
					if (maxRetries != default && tries >= maxRetries)
					{
						throw;
					}

					tries++;
				}
			}
		}
	}
}
