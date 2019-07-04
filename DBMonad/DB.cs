using Optional;
using Sap.Data.Hana;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Data.SqlClient;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;
using TMDBConnector;

namespace DBMonad
{
    public enum ServerPlatform
    {
        Sql = 0,
        Hana = 1
    }

    public abstract class DbParameter
    {
        public readonly string ParameterName;
        public readonly SqlDbType SqlType;
        public readonly HanaDbType HanaType;
        public readonly Option<object> Value;
        public readonly Option<int> Size;

        public DbParameter(string parameterName, SqlDbType sqlType, HanaDbType hanaType, object value = null, int? size = null)
        {
            ParameterName = parameterName;
            SqlType = sqlType;
            HanaType = hanaType;
            Value = value.SomeNotNull();
            Size = size.ToOption();
        }
        public class NVarChar : DbParameter
        {
            public NVarChar(string parmeterName, string value = null, int? size = null) :
                base(parmeterName, SqlDbType.NVarChar, HanaDbType.NVarChar, value, size)
            { }
        }
        public class SmallInt : DbParameter
        {
            public SmallInt(string parmeterName, int? value = null) :
                base(parmeterName, SqlDbType.SmallInt, HanaDbType.SmallInt, value)
            { }
        }

        public class BigInt : DbParameter
        {
            public BigInt(string parmeterName, int? value = null) :
                base(parmeterName, SqlDbType.BigInt, HanaDbType.BigInt, value)
            { }
        }

        public class Boolean : DbParameter
        {
            public Boolean(string parmeterName, bool? value = null, int? size = null) :
                base(parmeterName, SqlDbType.Bit, HanaDbType.Boolean, value, size)
            { }
        }

        public class DateTime : DbParameter
        {
            public DateTime(string parmeterName, System.DateTime? value = null) :
                base(parmeterName, SqlDbType.DateTime, HanaDbType.TimeStamp, value)
            { }
        }

        public class Decimal : DbParameter
        {
            public Decimal(string parmeterName, decimal? value = null) :
                base(parmeterName, SqlDbType.Decimal, HanaDbType.Decimal, value)
            { }
        }
        public class Text : DbParameter
        {
            public Text(string parmeterName, string value = null) :
                base(parmeterName, SqlDbType.NText, HanaDbType.Text, value)
            { }
        }
        public class VarBinary : DbParameter
        {
            public VarBinary(string parmeterName, byte[] value = null, int? size = null) :
                base(parmeterName, SqlDbType.VarBinary, HanaDbType.VarBinary, value, size)
            { }
        }
        /// 
        /// The BLOB data type is used to store large amounts of binary data
        /// 
        public class BLOB : DbParameter
        {
            public BLOB(string parmeterName, byte[] value = null) :
                base(parmeterName, SqlDbType.Image, HanaDbType.Blob, value)
            { }
        }
    }

    public abstract class ConnectionAuth
    {
        public class UserAndPass : ConnectionAuth
        {
            public readonly string UserName;
            public readonly string Password;
            public UserAndPass(string userName, string password) => (UserName, Password) = (userName, password);
            public void Deconstruct(out string userName, out string password) => (userName, password) = (UserName, Password);
        }
        public class IntegratedSecurity : ConnectionAuth { }
    }

    public class DBConnectionString
    {
        public readonly ServerPlatform ServerPlatform;
        public readonly string ConnectionString;
        public readonly Assembly AssemblyWithEmbResourcesQueries;

        public DBConnectionString(ServerPlatform serverPlatform, string connectionString)=>
            (ServerPlatform, ConnectionString, AssemblyWithEmbResourcesQueries) = 
            (serverPlatform, connectionString, DB.GetCallerAssembly());

        public DBConnectionString(ServerPlatform serverPlatform, string connectionString, Assembly queriesAssembly)=>
            (ServerPlatform, ConnectionString, AssemblyWithEmbResourcesQueries) = 
            (serverPlatform, connectionString, queriesAssembly);
        public void Deconstruct(out ServerPlatform sp, out string cs) =>
            (sp, cs) = (ServerPlatform, ConnectionString);

        public override string ToString()=> ConnectionString;
    }

    public class DBConnectionData : DBConnectionString
    {
        public readonly string Server;
        public readonly ConnectionAuth Auth;
        public readonly Option<string> Database;

        public DBConnectionData(ServerPlatform serverPlatform, string server, ConnectionAuth auth, Option<string> database = new Option<string>())
            : base(serverPlatform, DB.BuildConnectionString(serverPlatform, server, auth, database)) =>
            (Server, Auth, Database) = (server, auth, database);
        public DBConnectionData(ServerPlatform serverPlatform, string server, ConnectionAuth auth, string database)
            : base(serverPlatform, DB.BuildConnectionString(serverPlatform, server, auth, database.Some())) =>
            (Server, Auth, Database) = (server, auth, database.SomeNotNull());
        public void Deconstruct(out string server, out ConnectionAuth auth, out Option<string> database) =>
            (server, auth, database) = (Server, Auth, Database);

        public string GetDatabaseOrThrow() =>
            Database.ValueOr(() => throw new ArgumentException("DBConnectionData: database value is empty"));

        public DBConnectionData SwitchDB(string db) => new DBConnectionData(ServerPlatform, Server, Auth, db);
        public DBConnectionData SwitchAuth(string user, string password) =>
            new DBConnectionData(ServerPlatform, Server, new ConnectionAuth.UserAndPass(user, password), Database);
    }

    public static partial class DB
    {

        #region Connection
        public static string BuildConnectionString(ServerPlatform serverPlatform, string server, ConnectionAuth auth, Option<string> database = new Option<string>())
        {
            var dbStr =
                database.Map(db =>
                    serverPlatform == ServerPlatform.Sql ?
                    $"Initial Catalog={database};" : $"CS={database};"
                ).ValueOr("");

            var authStr =
                auth is ConnectionAuth.UserAndPass up ?
                    serverPlatform == ServerPlatform.Sql ?
                    $"uid={up.UserName};pwd={up.Password};"
                    : $"UserID={up.UserName};Password={up.Password};"
                : "Integrated Security=True;";

            return $"Server={server};{dbStr}{authStr}";
        }

        public static DbConnection GetConnection(DBConnectionString cs) => GetConnection(cs.ServerPlatform, cs.ConnectionString);
        public static DbConnection GetConnection(ServerPlatform serverPlatform, string connectionString)
        {
            return serverPlatform == ServerPlatform.Sql ?
                (DbConnection)new SqlConnection(connectionString)
                : new HanaConnection(connectionString);
        }

        public static T WithConnection<T>(ServerPlatform serverPlatform, string connectionString, Func<DbConnection, T> inOpenConnection)
        {
            using (var con = GetConnection(serverPlatform, connectionString))
            {
                con.Open();
                return inOpenConnection(con);
            }
        }
        public static Option<T, TError> WithTransaction<T, TError>(ServerPlatform serverPlatform, string connectionString, Func<DBState, Option<T, TError>> inOpenConnection)
        {
            using (var con = GetConnection(serverPlatform, connectionString))
            {
                con.Open();
                using (var t = con.BeginTransaction())
                    try
                    {
                        var res = inOpenConnection(new DBState(con, t.Some()));

                        if (res.HasValue) t.Commit();
                        else t.Rollback();

                        return res;
                    }
                    catch (Exception)
                    {
                        t.Rollback();
                        throw;
                    }

            }
        }

        public static T WithTransaction<T>(ServerPlatform serverPlatform, string connectionString, Func<DBState, T> inOpenConnection)
        {
            using (var con = GetConnection(serverPlatform, connectionString))
            {
                con.Open();
                using (var t = con.BeginTransaction())
                    try
                    {
                        var res = inOpenConnection(new DBState(con, t.Some()));
                        t.Commit();
                        return res;
                    }
                    catch (Exception e)
                    {
                        t.Rollback();
                        throw;
                    }

            }
        }

        #endregion

        #region DataAdapter

        public abstract class DACommand
        {
            //None- will set columns based on the parameter name
            //If parameter is not mentioned in the map the column name will set based on the parameter name
            //If column name is set as none no value will be set for this column
            public Dictionary<string, Option<string>> DbParamToColumnNameMap = new Dictionary<string, Option<string>>();
            public DbCommand cmd;
            public class Select : DACommand { }
            public class Update : DACommand { }
            public class Insert : DACommand { }
            public class Delete : DACommand { }
        }

        public static DACommand.Select ToDASelectCommand
            (this DbCommand c, Dictionary<string, Option<string>> pToCn = null) =>
            new DACommand.Select { cmd = c, DbParamToColumnNameMap = pToCn ?? new Dictionary<string, Option<string>>() };
        public static DACommand.Update ToDAUpdateCommand
            (this DbCommand c, Dictionary<string, Option<string>> pToCn = null) =>
            new DACommand.Update { cmd = c, DbParamToColumnNameMap = pToCn ?? new Dictionary<string, Option<string>>() };
        public static DACommand.Insert ToDAInsertCommand
            (this DbCommand c, Dictionary<string, Option<string>> pToCn = null) =>
            new DACommand.Insert { cmd = c, DbParamToColumnNameMap = pToCn ?? new Dictionary<string, Option<string>>() };
        public static DACommand.Delete ToDADeleteCommand
            (this DbCommand c, Dictionary<string, Option<string>> pToCn = null) =>
            new DACommand.Delete { cmd = c, DbParamToColumnNameMap = pToCn ?? new Dictionary<string, Option<string>>() };


        public static DbDataAdapter DataAdapter(DbCommand command) => DataAdapter(command.ToDASelectCommand());
        public static DbDataAdapter AddCommand(this DbDataAdapter da, DACommand command)
        {
            if (command is DACommand.Select) da.SelectCommand = command.cmd;
            if (command is DACommand.Update) da.UpdateCommand = command.cmd;
            if (command is DACommand.Insert) da.InsertCommand = command.cmd;
            if (command is DACommand.Delete) da.DeleteCommand = command.cmd;
            return da;
        }

        public static DbDataAdapter DataAdapter(DACommand command)
        {
            DbDataAdapter dataAdapter;
            if (command.cmd is SqlCommand sqlC)
            {
                dataAdapter = new SqlDataAdapter();
                sqlC.Parameters.Cast<SqlParameter>().ToList().ForEach(p =>
                {
                    if (command.DbParamToColumnNameMap.TryGetValue(p.ParameterName, out Option<string> maybeColumnName))
                        maybeColumnName.MatchSome(cn => p.SourceColumn = cn);
                    else
                        p.SourceColumn = p.ParameterName.Replace("@", "");
                });
            }
            else if (command.cmd is HanaCommand hanaC)
            {
                dataAdapter = new HanaDataAdapter();
                hanaC.Parameters.Cast<HanaParameter>().ToList().ForEach(p =>
                {
                    if (command.DbParamToColumnNameMap.TryGetValue(p.ParameterName, out Option<string> maybeColumnName))
                        maybeColumnName.MatchSome(cn => p.SourceColumn = cn);
                    else
                        p.SourceColumn = p.ParameterName;
                });
            }
            else
                throw new NotImplementedException(
                    $"DBDataAdapter for command of type {command.cmd.GetType().FullName}, is not yet implemented"
                );

            return dataAdapter.AddCommand(command);
        }

        public static Func<DbDataAdapter, int> ExecuteAndDispose(Func<DbDataAdapter, int> daMethodToExecute) =>
            da =>
            {
                var result = daMethodToExecute(da);
                da.Dispose();
                return result;
            };




        #endregion

        #region DbCommand

        public static Func<DBState, DbCommand> Command(string sqlQuery, string hanaQuery, params DbParameter[] ps) =>
            dbState => Command(dbState, sqlQuery, hanaQuery, ps);

        public static DbCommand Command(DbConnection dbCon, string sqlQuery, string hanaQuery, params DbParameter[] ps) =>
            Command(new DBState(dbCon), sqlQuery, hanaQuery, ps);

        public static DbCommand Command(DBState dbState, string sqlQuery, string hanaQuery, params DbParameter[] ps)
        {
            var command = dbState.Connection.Match<DbCommand>(
                sqlC => new SqlCommand(sqlQuery, sqlC),
                hanaC => new HanaCommand(hanaQuery, hanaC)
            );

            dbState.Transaction.MatchSome(t => command.Transaction = t);

            ps.ToList().ForEach(p =>
            {
                var param = dbState.Connection.Match<System.Data.Common.DbParameter>(
                    sql => new SqlParameter(sqlParamName(p.ParameterName), p.SqlType),
                    hana => new HanaParameter(hanaParamName(p.ParameterName), p.HanaType)
                );

                param.Value = p.Value.ValueOr(DBNull.Value);
                p.Size.MatchSome(s => param.Size = s);
                command.Parameters.Add(param);
            });

            return command;
        }


        public static Func<DBState, DbCommand> CommandFromFile(string fileName, params DbParameter[] ps) =>
            dbState => CommandFromFile(dbState, fileName, ps);

        public static DbCommand CommandFromFile(DBState dbState, string fileName, params DbParameter[] ps)
        {
            var fileExtension = dbState.Connection.Match(sql => "sql", hana => "hana");

            var query = QueryFromFile( 
                dbState.AssemblyWithEmbResourcesQueries,
                fileName, fileExtension
            );

            return Command(dbState, query, query, ps);
        }
        //Command extensions
        public static DbCommand SetParameterValue(this DbCommand c, string parameterName, object value)
        {
            if (c is SqlCommand sqlC)
                sqlC.Parameters[sqlParamName(parameterName)].Value = value;

            if (c is HanaCommand sqlH)
                sqlH.Parameters[hanaParamName(parameterName)].Value = value;

            return c;
        }


        //Helpers
        private static string sqlParamName(string pn) => $"@{pn}";
        private static string hanaParamName(string pn) => pn;


        #endregion

        #region Query from files


        public static Assembly GetCallerAssembly()
        {
            //Use stacktrace to find the first assembly in the call stack that is not 
            //This assembly (DBConnector)
            //This is should be a more expensive operation should be executed as little as possible
            var currentAssembly = Assembly.GetExecutingAssembly();
            var callerAssemblies = 
                new StackTrace().GetFrames()
                .Select(x => x.GetMethod().ReflectedType.Assembly).Distinct()
                .First(x => x.FullName != currentAssembly.FullName);

            return callerAssemblies;
        }

        public static string QueryFromFile(Assembly assemblyWithEmbResourcesQueries, string name, string queryFileExtension)
        {
            string fileName = $"{name}.{queryFileExtension}";

            var asm = assemblyWithEmbResourcesQueries;

            string fullName = 
                asm.GetManifestResourceNames().FirstOrDefault(r => r.EndsWith(fileName)).SomeNotNull()
                .ValueOr(()=> throw new Exception($"DBConnector:QueryFromFile:" +
                $"Can not find {fileName} resource query, in {asm.FullName} Assembly.") );

            Stream stream = asm.GetManifestResourceStream(fullName);
            var sr = new StreamReader(stream);

            string retVal = sr.ReadToEnd();

            sr.Close();
            sr.Dispose();
            stream.Close();
            stream.Dispose();

            return retVal;
        }

        public static string QueryFromFile(string name, string queryFileExtension)=>
            QueryFromFile(Assembly.GetExecutingAssembly(), name, queryFileExtension);

        #endregion

        #region Helpers

        /// <summary>
        /// Use Read on the data reader and convert it to dictionary 
        /// if read returns false None is return else Dictionary
        /// </summary>
        public static Queries<Option<Dictionary<string,object>>> ReadRow(this Queries<DbDataReader> queryResult)
        {
            return queryResult.Map(dr =>
                dr.SomeWhen(r => r.Read())
                .Map(r =>
                    Enumerable.Range(0, r.FieldCount)
                    .ToDictionary(r.GetName, r.GetValue)
                )
            );
        }

        public static T Match<T>(this DbConnection c, Func<SqlConnection, T> sqlHandler, Func<HanaConnection, T> hanaHandler) =>
            c is SqlConnection sqlC ? sqlHandler(sqlC) :
            c is HanaConnection hanaC ? hanaHandler(hanaC) :
            throw new NotImplementedException($"DBConnection of type:{c}, is not supported yet");
        #endregion

    }

    //Monadic type 
    public delegate (T Value, DBState DbState) Queries<T>(DBState dbState);
    public struct DBState
    {
        public DbConnection Connection;
        public Option<DbTransaction> Transaction;
        public Assembly AssemblyWithEmbResourcesQueries;
        public DBState(DbConnection c, Option<DbTransaction> t = new Option<DbTransaction>()) =>
            (Connection, Transaction,AssemblyWithEmbResourcesQueries) = (c, t, DB.GetCallerAssembly());

        public DBState(DbConnection c, Assembly assemblyWithEmbResourcesQueries, Option<DbTransaction> t = new Option<DbTransaction>()) =>
                  (Connection, Transaction, AssemblyWithEmbResourcesQueries) = 
            (c, t, assemblyWithEmbResourcesQueries);

        public void Deconstruct(out DbConnection c, out Option<DbTransaction> t) =>
            (c, t) = (Connection, Transaction);
    }


    //Monadic interface 
    public static partial class DB
    {
        #region Return
        public static Queries<Unit> NewQueries() => s => (Prelude.unit, s);
        public static Queries<T> NewQueries<T>() => s => (default(T), s);
        public static Queries<T> ToQueries<T>(this T value) =>
            dbState => (value, dbState);

        public static Queries<T> Then<T>(this Func<DBState, DbCommand> commandFactory, Func<DbCommand, T> commandToExecute) =>
            dbState => (commandToExecute(commandFactory(dbState)), dbState);

        //DataAdapters 
        public static Queries<int> ThenDataAdapterFill(this Func<DBState, DbCommand> commandFactory, DataTable dt) =>
            commandFactory.Then(c => ToDASelectCommand(c)).Map(DataAdapter)
            .Map(ExecuteAndDispose(da => da.Fill(dt)));
        public static Queries<int> ThenDataAdapterFill(this Func<DBState, DbCommand> commandFactory, DataSet ds) =>
            commandFactory.Then(c => ToDASelectCommand(c)).Map(DataAdapter)
            .Map(ExecuteAndDispose(da => da.Fill(ds)));

        public static Queries<int> ThenDataAdapterUpdate(this Func<DBState, DbCommand> commandFactory,
            DataTable dt, Dictionary<string, Option<string>> paramToSourceColumn = null) =>
            commandFactory.Then(c => ToDAUpdateCommand(c, paramToSourceColumn)).Map(DataAdapter)
            .Map(ExecuteAndDispose(da => da.Update(dt)));

        public static Queries<int> ThenDataAdapterInsert(this Func<DBState, DbCommand> commandFactory, DataTable dt) =>
            commandFactory.Then(c => ToDAInsertCommand(c)).Map(DataAdapter)
            .Map(ExecuteAndDispose(da => da.Update(dt)));

        public static Queries<int> ThenDataAdapterDelete(this Func<DBState, DbCommand> commandFactory, DataTable dt) =>
            commandFactory.Then(c => ToDADeleteCommand(c)).Map(DataAdapter)
            .Map(ExecuteAndDispose(da => da.Update(dt)));
        #endregion

        #region Bind
        public static Queries<TSelector> FlatMap<T, TSelector>(
            this Queries<T> source,
            Func<T, Queries<TSelector>> selector) =>
            SelectMany(source, selector, (t, tSelector) => tSelector);

        //Error handling bind - Extract 2 layers of monads(Queries and then Option)
        public static Queries<Option<TSelector, TError>> FlatMapWithError<T, TSelector, TError>(
            this Queries<Option<T, TError>> source,
            Func<T, Queries<Option<TSelector, TError>>> selector) =>
                oldState =>
                {
                    var (Value, DbState) = source(oldState);
                    return Value.Match(
                        oldValue => selector(oldValue)(DbState)
                        , error => (Option.None<TSelector, TError>(error), DbState)
                    );
                };

        public static Queries<TResult> SelectMany<T, TSelector, TResult>(
            this Queries<T> source,
            Func<T, Queries<TSelector>> selector,
            Func<T, TSelector, TResult> resultSelector) =>
        oldState =>
        {
            var (value, dbState) = source(oldState);
            var (newValue, newDbState) = selector(value)(dbState);
            return (resultSelector(value, newValue), newDbState); // Output new Queries.
        };

        #endregion

        #region Map/Functor
        public static Queries<TResult> Map<T, TResult>(this Queries<T> source, Func<T, TResult> selector) =>
            Select(source, selector);

        public static Queries<Option<TResult, TError>> MapWithError<T, TResult, TError>(
            this Queries<Option<T, TError>> source, Func<T, TResult> selector) =>
            source.FlatMapWithError<T, TResult, TError>(v =>
                s => (selector(v).Some<TResult, TError>(), s)
            );

        public static Queries<TResult> Select<T, TResult>(this Queries<T> source, Func<T, TResult> selector) =>
                oldState =>
                {
                    (T Value, DBState state) = source(oldState);
                    DBState newState = state;
                    return (selector(Value), newState); // Output new state.
                };

        //TODO: add error handling functor
        #endregion

        #region Execute

        public static T Run<T>(this Queries<T> queries, ServerPlatform sp, string connectionString) =>
            WithConnection(sp, connectionString, con =>
                 queries(new DBState(con, Option.None<DbTransaction>())).Value
            );
        public static T Run<T>(this Queries<T> queries, DBConnectionString cs) =>
            WithConnection(cs.ServerPlatform, cs.ConnectionString, con =>
                 queries(new DBState(con, Option.None<DbTransaction>())).Value
            );
        public static T RunWithTransaction<T>(this Queries<T> queries, ServerPlatform sp, string connectionString) =>
            WithTransaction(sp, connectionString, dbState =>
                 queries(dbState).Value
            );
        public static T RunWithTransaction<T>(this Queries<T> queries, DBConnectionString cs) =>
            WithTransaction(cs.ServerPlatform, cs.ConnectionString, dbState =>
                 queries(dbState).Value
            );

        public static Option<T, TError> RunWithTransaction<T, TError>(this Queries<Option<T, TError>> queries, DBConnectionString cs) =>
            WithTransaction(cs.ServerPlatform, cs.ConnectionString, dbState =>
                 queries(dbState).Value
            );


        #endregion

        #region State manipulation
        public static Queries<Unit> SetTransaction(Func<DbConnection, DbTransaction, DbTransaction> handleState) =>
            oldState =>
            {
                var newTrans = oldState.Transaction.Map(oldT =>
                {
                    var newT = handleState(oldState.Connection, oldT);
                    if (!ReferenceEquals(oldT, newT)) oldT.Dispose();
                    return newT;
                });
                return (Prelude.unit, new DBState(oldState.Connection, newTrans));
            };
        #endregion

        public static Queries<Option<T, TError>> Catch<T, TException, TError>
            (this Queries<T> queries, Func<TException, TError> handleException)
            where TException : Exception =>
            s =>
            {
                try
                {
                    var (Value, DbState) = queries(s);
                    return (Value.Some<T, TError>(), DbState);
                }
                catch (TException e)
                {
                    return (Option.None<T, TError>(handleException(e)), s);
                }
            };


        public static Queries<T> RunIf<T>(this Queries<T> queries, bool pred) =>
            pred ? queries : s => (default(T), s);
        public static Queries<T> RunIf<T>(this Queries<T> queries, bool pred, T alternativeValue) =>
            pred ? queries : s => (alternativeValue, s);
    }

    //Usage Example
    public static partial class DB
    {
        internal static void Workflow()
        {
            Queries<string> a = Command("select '123'", "select '123'").Then(c => (string)c.ExecuteScalar());
            Queries<string> d1 = s => ("", s);
            Queries<bool> d2 = s => (false, s);
            Queries<int> d3 = s => (2, s);


            Queries<string> qs =
                from x in a
                from x2 in Command("sdlfkj", "fdslkfjds", new DbParameter.BigInt("IN", 13))
                          .Then(c => (string)c.ExecuteScalar())
                from y in CommandFromFile("bla", new DbParameter.NVarChar("variable", x2))
                          .Then(c => c.ExecuteScalar()).Map(Convert.ToString)
                          .RunIf(x2 == "value from q1", "alternative value")
                    //If not running with transaction this line wont execute
                from _ in SetTransaction((c, t) => { t.Commit(); return c.BeginTransaction(); })
                select x2;

            qs.RunWithTransaction(ServerPlatform.Hana, "connectionstring");


            var bolOfRead =
            Command("sfds", "sdlfkjsdf").Then(c => c.ExecuteReader())
                .Map(dr => dr.Read())
                .Run(ServerPlatform.Hana, "sdfsdf");

            var str =
                CommandFromFile("file").Then(c => (int)c.ExecuteScalar())
                .FlatMap(i =>
                    Command("select 'im string'", "select 'im hana string'", new DbParameter.BigInt("i", i))
                    .Then(c => (string)c.ExecuteScalar())
                )
                .RunWithTransaction(ServerPlatform.Hana, "sdfsdf");



        }
    }

}
