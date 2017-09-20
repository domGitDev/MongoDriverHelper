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

namespace ONEsVIBE.Mongo
{
    public class MongoHelper
    {
        private const string server = "YOUR SERVER";
        private const int port = 0000; //YOUR PORT NUMBER
        private const string databaseName = "YOUR DBNAME";
        private const string APP_TEMP_DIR = "YOUR APP TEMP DIR";

        private static MongoHelper instance;
        private static MongoClient client;
        private static IMongoDatabase database;
        private static bool isconnected = false;

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


        private static MongoClient ConnectUsingCredential(string dbname, string user, string password)
        {
            var identity = new MongoInternalIdentity(dbname, user);
            var evidence = new PasswordEvidence(password);
            var credential = new MongoCredential("SCRAM-SHA-1", identity, evidence);

            var settings = new MongoClientSettings
            {
                Credentials = new[] { credential },
                Server = new MongoServerAddress(server, port),
                ServerSelectionTimeout = TimeSpan.FromSeconds(5)
			};

			return new MongoClient(settings);
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

            isconnected = false;

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

          isconnected = false;
                client = ConnectUsingCredential(dbname, user, password);
        }


        public async Task WaitConnectionTimeoutAsync()
        {
            if (client == null)
                return;
            int timeOut = 5;

            while (timeOut > 0)
            {
                if (client.Cluster.Description.State == MongoDB.Driver.Core.Clusters.ClusterState.Connected)
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
                return isconnected;
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


        public async Task<List<T>> GetPostsAsync<T>(string collectionName, string sortByAttribute = "PostTime")
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


        public static void CloseConnection()
        {
            if (client != null && client.Cluster.Description.State == MongoDB.Driver.Core.Clusters.ClusterState.Connected)
            {
                client.Cluster.Dispose();
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


        public async Task<ObjectId> UploadStreamAsync(Stream stream, string bucketname, int chuncksSize = 64512)
        {
            ObjectId id = ObjectId.Empty;

            var bucket = new GridFSBucket(database, new GridFSBucketOptions
            {
                ChunkSizeBytes = chuncksSize,
                BucketName = bucketname
            });
            var path = MediaTempFilename(APP_TEMP_DIR, "jpg");
            using (var writestream = new FileStream(path, FileMode.OpenOrCreate, FileAccess.Write))
            {
                var size = stream.Length;
                var buffer = new byte[size];
                await stream.ReadAsync(buffer, 0, (int)size);
                await writestream.WriteAsync(buffer, 0, (int)size);
            }
            if (!File.Exists(path))
                return id;
            return await UploadFileAsync(path, bucketname, chuncksSize);
        }


        public async Task<bool> DownloadFileAsync(ObjectId id, Stream OutStrem, string bucketname = "voices")
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


        public async Task<bool> DownloadFileAsync(ObjectId id, string outpath, string bucketname = "voices")
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


        public async Task<List<UserData>> FindeUser(string username, string collectionName = "usersdata")
        {
            var collection = database.GetCollection<UserData>(collectionName);
            var filter = Builders<UserData>.Filter.Where(x => x.Username == username);
            var cursor = await collection.FindAsync<UserData>(filter);
            return await cursor.ToListAsync<UserData>();
        }


        public static string MediaTempFilename(string dirname, string ext = "jpg")
        {
            var dataPath = FileAccessHelper.GetLocalFilePath(dirname, true);
            var filename = Path.GetFileName(Path.GetTempFileName());
            return Path.Combine(dataPath, string.Format("{0}.{1}", filename, ext.Replace(".", "")));
        }


        public static async Task<bool> CreateUserAccount(string username, string password, string email, string gender)
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
                            {"db", "onesvibedb"}
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

		public async Task<bool> CheckUsernameIsAvailable(string username)
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
                if (users.Count > 0)
                    return false;
            }
            catch (MongoCommandException)
            {
                
            }
            return true;
		}
    }
}
