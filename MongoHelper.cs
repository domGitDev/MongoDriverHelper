// Author: Dom Silva
// Email: silavdom2014@gmail.com

using System;
using System.IO;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Newtonsoft.Json;

using MongoDB.Bson;
using MongoDB.Driver;
using MongoDB.Driver.GridFS;

using ONEsVIBE.iOS;
using MongoDB.Bson.Serialization;

namespace Mongo
{
    public class MongoHelper
    {
        private const string server = "SERVER_ADDRESS";
        private const int port = PORT_NUMBER;
        private const string databaseName = "DB_NAME";

        private static MongoHelper instance;
        private static MongoClient client;
        private static IMongoDatabase database;

        public string StatusMessage { get; set; }


        private MongoHelper()
        {

        }


        public static MongoHelper Instance
        {
            get
            {
                if (instance == null)
                    instance = new MongoHelper();
                return instance;
            }
        }


        public MongoClient Client
        {
            get
            {
                if (client == null)
                    throw new Exception("MongoHelper not initialized!");
                return client;
            }
        }


        private static MongoClient ConnectUsingCredential(string dbname, string user, string password, int tryCount=2)
        {
            var identity = new MongoInternalIdentity(dbname, user);
            var evidence = new PasswordEvidence(password);
            var credential = new MongoCredential("SCRAM-SHA-1", identity, evidence);

            var settings = new MongoClientSettings
            {
                Credentials = new[] { credential },
                Server = new MongoServerAddress(server, port),
                ServerSelectionTimeout = TimeSpan.FromSeconds(10)
			};

            MongoClient clientInstance = null;
            try
            {
                clientInstance = new MongoClient(settings);
            }
            catch(MongoClientException e)
            {
                if(tryCount > 0)
                    ConnectUsingCredential(dbname, user, password, tryCount-1);
                Console.WriteLine(e.ToString());
            }
            return clientInstance;
        }


        public static void Initialize(string connectionString, bool force = false)
        {
            if (connectionString == null)
                throw new ArgumentNullException(nameof(connectionString));

            // TODO: dispose any existing connection
            if (!force && instance != null)
            {
                CloseConnection();
                return;
            }

            instance = new MongoHelper();

            MongoClientSettings settings = MongoClientSettings.FromUrl(new MongoUrl(connectionString));
            client = new MongoClient(settings);

        }


		public static void InitializeWithCredential(string dbname, string user, string password, bool force = false)
		{
			// TODO: dispose any existing connection
			if (!force && instance != null)
			{
				CloseConnection();
				return;
			}

			instance = new MongoHelper();
            client = ConnectUsingCredential(dbname, user, password);
		}


        public async Task WaitConnectionTimeoutAsync()
        {
            if (client == null)
                return;
            int timeOut = 10;

            while (timeOut > 0)
            {
                if (isConnected)
                {
                    database = client.GetDatabase(databaseName);
                    isconnected = true;
                    return;
                }
                await Task.Delay(1000);
                timeOut -= 1;
            }
        }


        public static void InitRemoteMongoDB(string user, string password, bool force = true)
        {
            InitializeWithCredential(databaseName, user, password, force);
        }


        public bool isConnected
        {
            get
            {
                if (client != null)
                    return client.Cluster.Description.State == MongoDB.Driver.Core.Clusters.ClusterState.Connected;
                return false;
            }
        }


        public IMongoCollection<T> PostCollection<T>(string name)
        {
            var collections = database.GetCollection<T>(name);
            return collections;
        }


        public async Task InsertPostAsync<T>(string collectionName, T data)
        {
            await PostCollection<T>(collectionName).InsertOneAsync(data);
        }


        public async Task<List<T>> GetPostsAsync<T>(string collectionName, string sortByAttribute)
        {
            var sort = Builders<T>.Sort.Descending(sortByAttribute);
            var options = new FindOptions<T>
            {
                Sort = sort
            };

            var collection = database.GetCollection<T>(collectionName);
            var cursor = await collection.FindAsync(new BsonDocument(), options);
            return await cursor.ToListAsync<T>();
        }


		public async Task<List<T>> FindByPropertyAsync<T>(
				string propertyValue, string propertyName, string collectionName)
		{
			var collection = database.GetCollection<T>(collectionName);
				var filter = new BsonDocument(propertyName, propertyValue);
			var cursor = await collection.FindAsync<T>(filter);
			return await cursor.ToListAsync<T>();
		}


        public async Task<ReplaceOneResult> ReplaceOneByPropertyAsync<T>(
			T data, string propertyValue, string propertyName, string collectionName)
		{
			var collection = database.GetCollection<T>(collectionName);
			var filter = Builders<T>.Filter.Eq(propertyName, propertyValue);
			return await collection.ReplaceOneAsync(filter, data);
		}



		public static void CloseConnection()
        {
            if (client != null && client.Cluster.Description.State == MongoDB.Driver.Core.Clusters.ClusterState.Connected)
            {
                //client.Cluster.Dispose();
                client = null;
            }
            instance = null;
        }


        public async Task<ObjectId> UploadFileAsync(string path, string bucketname, int chuncksSize = 64512)
        {
            var bucket = new GridFSBucket(database, new GridFSBucketOptions
            {
                ChunkSizeBytes = chuncksSize,
                BucketName = bucketname
            });

            using (var stream = new FileStream(path, FileMode.Open, FileAccess.Read))
            {
                return await bucket.UploadFromStreamAsync(path, stream);
            }
        }


        public async Task<ObjectId> UploadStreamAsync(
            Stream stream, string outpath, string bucketname, int chuncksSize = 64512)
        {
            ObjectId id = ObjectId.Empty;

            var bucket = new GridFSBucket(database, new GridFSBucketOptions
            {
                ChunkSizeBytes = chuncksSize,
                BucketName = bucketname
            });

            using (var writestream = new FileStream(outpath, FileMode.OpenOrCreate, FileAccess.Write))
            {
                var size = stream.Length;
                var buffer = new byte[size];
                await stream.ReadAsync(buffer, 0, (int)size);
                await writestream.WriteAsync(buffer, 0, (int)size);
            }
            if (!File.Exists(outpath))
                return id;
            return await UploadFileAsync(outpath, bucketname, chuncksSize);
        }


        public async Task<bool> DownloadFileAsync(ObjectId id, Stream OutStrem, string bucketname)
        {
            var bucket = new GridFSBucket(database, new GridFSBucketOptions
            {
                ChunkSizeBytes = 64512,
                BucketName = bucketname
            });
            var filter = Builders<GridFSFileInfo<ObjectId>>.Filter.Where(x => x.Id == id);
            var cursor = await bucket.FindAsync(filter);
            var result = await cursor.ToListAsync();

            if (result.Count == 0) return false;

            await bucket.DownloadToStreamAsync(id, OutStrem);
            return true;
        }


        public async Task<bool> DownloadFileAsync(ObjectId id, string outpath, string bucketname)
        {
            var bucket = new GridFSBucket(database, new GridFSBucketOptions
            {
                ChunkSizeBytes = 64512,
                BucketName = bucketname
            });
            var filter = Builders<GridFSFileInfo<ObjectId>>.Filter.Where(x => x.Id == id);
            var cursor = await bucket.FindAsync(filter);
            var result = await cursor.ToListAsync();

            if (result.Count == 0) return false;

            using (var outStream = new FileStream(outpath, FileMode.OpenOrCreate, FileAccess.Write))
            {
                await bucket.DownloadToStreamAsync(id, outStream);

            }
            return true;
        }


        public static async Task<bool> CreateUserAccount(string dbname, string username, string password, string email, string gender)
        {
            var command = new BsonDocument {
                { "createUser", username },
                { "pwd", password },
                {"customData", new BsonDocument{
                        { "email", email },
                        { "gender", gender}
                    }
                },
                { "roles", new BsonArray{
                        new BsonDocument{
                            {"role", "readWrite"},
                            {"db", dbname}
                        }
                    }
                } 
            };

            try
            {
                await database.RunCommandAsync<BsonDocument>(command);
            }
            catch(MongoCommandException)
            {
                return false;
            }
			return true;
        }


        public async Task<List<BsonDocument>> FindUser(string username)
        {
			var command = new BsonDocument
			{
				{"usersInfo", new BsonDocument{
						{"user", username},
						{"db", databaseName}
					}
				}
			};
			try
			{
				var result = await database.RunCommandAsync<BsonDocument>(command);
				var users = BsonSerializer.Deserialize<List<BsonDocument>>(result["users"].ToJson());
                return users;
			}
			catch (MongoCommandException)
			{

			}
            return new List<BsonDocument>();
        }

		public async Task<bool> CheckUsernameIsAvailable(string username)
		{
            var users = await FindUser(username);
            if (users.Count > 0)
                return false;
            return true;
		}
    }
}
