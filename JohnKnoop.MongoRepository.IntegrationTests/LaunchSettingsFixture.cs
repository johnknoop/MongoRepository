using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Docker.DotNet;
using Docker.DotNet.Models;
using JohnKnoop.MongoRepository;
using MongoDB.Driver;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace JohnKnoop.MongoRepository.IntegrationTests
{
	public class LaunchSettingsFixture : IDisposable
	{
		public LaunchSettingsFixture()
		{
			using (var file = File.OpenText("Properties\\launchSettings.json"))
			{
				var reader = new JsonTextReader(file);
				var jObject = JObject.Load(reader);

				var variables = jObject
					.GetValue("profiles")
					//select a proper profile here
					.SelectMany(profiles => profiles.Children())
					.SelectMany(profile => profile.Children<JProperty>())
					.Where(prop => prop.Name == "environmentVariables")
					.SelectMany(prop => prop.Value.Children<JProperty>())
					.ToList();

				foreach (var variable in variables)
				{
					Environment.SetEnvironmentVariable(variable.Name, variable.Value.ToString());
				}
			}
		}

		public void Dispose()
		{

		}
	}
}