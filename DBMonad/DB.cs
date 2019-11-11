using OneOf;
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
using System.Reactive;
using System.Reactive.Linq;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;
using TMDBConnector;

namespace DBMonad
{

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


    public class DBConnectionString<TCon> where TCon : DbConnection
    {
        public readonly string ConnectionString;
        public readonly Assembly AssemblyWithEmbResourcesQueries;

        public DBConnectionString(string connectionString)=>
            (ConnectionString, AssemblyWithEmbResourcesQueries) = 
            (connectionString, DB.GetCallerAssembly());

        public DBConnectionString(string connectionString, Assembly queriesAssembly)=>
            (ConnectionString, AssemblyWithEmbResourcesQueries) = 
            (connectionString, queriesAssembly);
        public void Deconstruct(out string cs) => cs = ConnectionString;

        public override string ToString()=> ConnectionString;

    }
    public static class DBConnectionString
    {
        public static string GetConnectionString<TCon1, TCon2>(this OneOf<DBConnectionString<TCon1>, DBConnectionString<TCon2>> css)
            where TCon1 : DbConnection, new()
            where TCon2 : DbConnection, new() =>
            css.Match(
                    c1 => c1.ConnectionString
                    , c2 => c2.ConnectionString
                );

    }

    public class DBConnectionData<TCon> : DBConnectionString<TCon> 
        where TCon : DbConnection
    {
        public readonly string Server;
        public readonly ConnectionAuth Auth;
        public readonly Option<string> Database;

        public DBConnectionData(string server, ConnectionAuth auth, Option<string> database = new Option<string>())
            : base(DB.BuildConnectionString<TCon>(server, auth, database)) =>
            (Server, Auth, Database) = (server, auth, database);
        public DBConnectionData(string server, ConnectionAuth auth, string database)
            : base(DB.BuildConnectionString<TCon>(server, auth, database.Some())) =>
            (Server, Auth, Database) = (server, auth, database.SomeNotNull());
        public void Deconstruct(out string server, out ConnectionAuth auth, out Option<string> database) =>
            (server, auth, database) = (Server, Auth, Database);

        public string GetDatabaseOrThrow() =>
            Database.ValueOr(() => throw new ArgumentException("DBConnectionData: database value is empty"));

        public DBConnectionData<TCon> SwitchDB(string db) => new DBConnectionData<TCon>(Server, Auth, db);
        public DBConnectionData<TCon> SwitchAuth(string user, string password) =>
            new DBConnectionData<TCon>(Server, new ConnectionAuth.UserAndPass(user, password), Database);
    }


    public static partial class DB
    {

        #region Connection
        public static string BuildConnectionString<TCon>(string server, ConnectionAuth auth, Option<string> database = new Option<string>())
            where TCon : DbConnection
        {
            var dbStr =
                database.Map(db =>
                    typeof(TCon).Name == "HanaConnection" ?
                    $"CS={db};":$"Initial Catalog={db};" 
                ).ValueOr("");

            var authStr =
                auth is ConnectionAuth.UserAndPass up ?
                    typeof(TCon).Name == "HanaConnection" ?
                    $"UserID={up.UserName};Password={up.Password};"
                    : $"uid={up.UserName};pwd={up.Password};"
                : "Integrated Security=True;";

            return $"Server={server};{dbStr}{authStr}";
        }

        public static TCon GetConnection<TCon>(DBConnectionString<TCon> cs)
            where TCon : DbConnection, new() 
            => GetConnection<TCon>(cs.ConnectionString);
        public static TCon GetConnection<TCon>(string connectionString) where TCon : DbConnection, new()
            => new TCon { ConnectionString = connectionString };

        public static T WithConnection<T, TCon>(string connectionString, Func<TCon, T> inOpenConnection)
            where TCon : DbConnection, new()
        {
            using (var con = GetConnection<TCon>(connectionString))
            {
                con.Open();
                return inOpenConnection(con);
            }
           
        }

        public static Option<T, TError> WithTransaction<T, TError, TCon>(string connectionString, Func<DBState<TCon>, Option<T, TError>> inOpenConnection)
            where TCon : DbConnection, new()
        {
            using (var con = GetConnection<TCon>(connectionString))
            {
                con.Open();
                using (var t = con.BeginTransaction())
                    try
                    {
                        var res = inOpenConnection(new DBState<TCon>(con, t.Some()));

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

        public static T WithTransaction<T,TCon>(string connectionString, Func<DBState<TCon>, T> inOpenConnection)
            where TCon : DbConnection, new()
        {
            using (var con = GetConnection<TCon>(connectionString))
            {
                con.Open();
                using (var t = con.BeginTransaction())
                    try
                    {
                        var res = inOpenConnection(new DBState<TCon>(con, t.Some()));
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

        public static Func<DBState<DbConnection>, DbCommand> Command(string query, params DbParameter[] ps) =>
            Command<DbConnection>(query, ps);
        public static Func<DBState<TCon>, DbCommand> Command<TCon>(string query , params DbParameter[] ps) 
            where TCon : DbConnection => 
            dbState => Command(dbState, query, ps);

        public static DbCommand Command<TCon>(TCon dbCon, string query, params DbParameter[] ps) 
            where TCon : DbConnection => 
            Command(new DBState<TCon>(dbCon), query, ps);

        public static DbCommand Command<TCon>(DBState<TCon> dbState, string query, params DbParameter[] ps)
            where TCon : DbConnection 
        {
            var cmd = dbState.Connection.CreateCommand();
            cmd.CommandText = query;
            dbState.Transaction.MatchSome(t => cmd.Transaction = t);
            cmd.Parameters.AddRange(ps);
            return cmd;
        }

        /// <summary>
        /// Create a factory function for a DBCommand which get the DBState 
        /// the command will be created based on embedded resource query file 
        /// that will be searched in dbState.AssemblyWithEmbResourcesQueries 
        /// based on fileName.[sql|hana] pattern - extension will be chosed based on dbState.Connection Type
        /// </summary>
        /// <param name="fileName">the query file name(without extensions)</param>
        /// <param name="ps"> DBMonad DBParameters which will be mapped to the correct DB Data provider Parameter</param>
        /// <returns></returns>
        public static Func<DBState<DbConnection>, DbCommand> CommandFromFile(string fileName, params DbParameter[] ps) =>
            dbState => CommandFromFile(dbState, fileName, ps);

        /// <summary>
        /// Create a factory function for a DBCommand which get the DBState 
        /// the command will be created based on embedded resource query file 
        /// that will be searched in dbState.AssemblyWithEmbResourcesQueries 
        /// based on fileName.[sql|hana] pattern - extension will be chosed based on dbState.Connection Type
        /// </summary>
        /// <param name="fileName">the query file name(without extensions)</param>
        /// <param name="replaceInQueryStringList">will be used as a String.Format parameters to replace the query string. 
        /// NOTE: this should not be used unless the source for this list is secured in order to avoid SQL injections</param>
        /// <param name="ps"> DBMonad DBParameters which will be mapped to the correct DB Data provider Parameter</param>
        /// <returns></returns>
         public static Func<DBState<DbConnection>, DbCommand> CommandFromFile(string fileName,string[] replaceInQueryStringList, params DbParameter[] ps) =>
            dbState => CommandFromFile(dbState, fileName, replaceInQueryStringList, ps);


        public static DbCommand CommandFromFile(DBState<DbConnection> dbState, string fileName, params DbParameter[] ps) =>
        CommandFromFile(dbState, fileName, new string[] { } , ps);

        /// <summary>
        /// Create DBCommand based on embedded resource query file 
        /// that will be searched in dbState.AssemblyWithEmbResourcesQueries 
        /// based on fileName.[sql|hana] pattern - extension will be chosed based on dbState.Connection Type
        /// </summary>
        /// <param name="fileName">the query file name(without extensions)</param>
        /// <param name="replaceInQueryStringList">will be used as a String.Format parameters to replace the query string. 
        /// NOTE: this should not be used unless the source for this list is secured in order to avoid SQL injections</param>
        /// <param name="ps"> DBMonad DBParameters which will be mapped to the correct DB Data provider Parameter</param>
        /// <returns></returns>
        public static DbCommand CommandFromFile<TCon>(DBState<TCon> dbState, string fileName, string[] replaceInQueryStringList, params DbParameter[] ps)
            where TCon : DbConnection
        {
            var fileExtension = typeof(TCon).Name.Replace("Connection","").ToLower();

            var query = string.Format(QueryFromFile( 
                dbState.AssemblyWithEmbResourcesQueries,
                fileName, fileExtension
            ), replaceInQueryStringList);

            return Command(dbState, query, ps);
        }

        public static DbCommand AddParameters(this DbCommand cmd, params DbParameter[] ps) =>
                ps.Select(dbP =>
                {
                    var p = cmd.CreateParameter();
                    p.ParameterName = dbP.ParameterName;
                    dbP.Value.MatchSome(v => p.Value = v);
                    dbP.Size.MatchSome(sz => p.Size = sz);
                    p.DbType = DbType.AnsiString;//TODO
                    return p;
                }).Aggregate(cmd, (acc, p) => { acc.Parameters.Add(p); return acc; });

        public static Func<TState, DbCommand> AddParameters<TState>(
            this Func<TState, DbCommand> commandFactory, 
            params DbParameter[] ps
            ) => s=> commandFactory(s).AddParameters(ps);

        //Helpers
        private static string sqlParamName(string pn) => $"@{pn}";
        private static string hanaParamName(string pn) => pn;


        #endregion

        #region Or/OneOf/Alternatives

        public static Func<OneOf<DBState<TCon>, DBState<TCon2>>, DbCommand> Or<TCon, TCon2>(
            this Func<DBState<TCon>, DbCommand> commandFactory, 
            Func<DBState<TCon2>, DbCommand> commandFactory2
            ) where TCon : DbConnection
            where TCon2 : DbConnection
            => ss => ss.Match(commandFactory, commandFactory2);

        public static Func<DBState<DbConnection>, DbCommand> Or<TCon>(
            this Func<DBState<DbConnection>, DbCommand> commandFactory,
            Func<DBState<TCon>, DbCommand> commandFactory2
            ) where TCon : DbConnection
            => s => s is DBState<TCon> s2 ? commandFactory2(s2) : commandFactory(s);

        public static Func<DBState<DbConnection>, DbCommand> Or<TCon>(
            this Func<DBState<TCon>, DbCommand> commandFactory,
             Func<DBState<DbConnection>, DbCommand> commandFactory2
            ) where TCon : DbConnection
            => s => s is DBState<TCon> s2 ? commandFactory(s2): commandFactory2(s);


        public static Func<OneOf<DBState<TCon>, DBState<TCon2>, DBState<TCon3>>, DbCommand> Or<TCon, TCon2, TCon3>(
            this Func<OneOf<DBState<TCon>, DBState<TCon2>>, DbCommand> commandsOptions,
            Func<DBState<TCon3>, DbCommand> commandFactory2
            ) where TCon : DbConnection
            where TCon2 : DbConnection
            where TCon3 : DbConnection
            => ss => ss.TryPickT2(out var s3, out var reminder) ?
                commandFactory2(s3) : commandsOptions(reminder);


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
        /// This function will return IEnumerable of the rows as DataReaders 
        /// Note that the connection must be open (or executed with in the queries monad)
        /// when iterating over the rows
        /// </summary>
        public static IEnumerable<DbDataReader> EnumerateRows(this DbCommand c)
        {
            using (var reader = c.ExecuteReader()) while (reader.Read()) yield return reader;
        }

        public static Dictionary<string, object> ToRow(this DbDataReader r) =>
            Enumerable.Range(0, r.FieldCount).ToDictionary(r.GetName, r.GetValue);

        /// <summary>
        /// Use Read on the data reader and convert it to dictionary 
        /// if read returns false None is return else Dictionary
        /// </summary>
        public static Queries<Option<Dictionary<string,object>>,TState> ReadRow<TState>(this Queries<DbDataReader, TState> queryResult)=>
            queryResult.Map(dr => dr.SomeWhen(r => r.Read()).Map(ToRow) );


        #region Values Convertions
        public static string ToString(object v) => v.CastObjTo<string>();
        public static int? ToInt(object v) => v.CastTo<int>();
        public static decimal? ToDecimal(object v) => v.CastTo<decimal>();

        //Casting and handling DBNulls
        private static T? CastTo<T>(this object dbValue) where T : struct
        {
            if (dbValue == DBNull.Value) return null;
            return (T)dbValue;
        }
        private static T CastObjTo<T>(this object dbValue) where T : class
        {
            if (dbValue == DBNull.Value) return null;
            return (T)dbValue;
        }
        #endregion

        public static T Match<T>(this DbConnection c, Func<SqlConnection, T> sqlHandler, Func<HanaConnection, T> hanaHandler) =>
            c is SqlConnection sqlC ? sqlHandler(sqlC) :
            c is HanaConnection hanaC ? hanaHandler(hanaC) :
            throw new NotImplementedException($"DBConnection of type:{c}, is not supported yet");
        #endregion

    }

    //Monadic type 
    public delegate (T Value, TState DbState) Queries<T ,TState>(TState dbState);

    public struct DBState<TCon> where TCon : DbConnection
    {
        public TCon Connection;
        public Option<DbTransaction> Transaction;
        public Assembly AssemblyWithEmbResourcesQueries;
        public DBState(TCon c, Option<DbTransaction> t = new Option<DbTransaction>()) =>
            (Connection, Transaction,AssemblyWithEmbResourcesQueries) = (c, t, DB.GetCallerAssembly());

        public DBState(TCon c, Assembly assemblyWithEmbResourcesQueries, Option<DbTransaction> t = new Option<DbTransaction>()) =>
                  (Connection, Transaction, AssemblyWithEmbResourcesQueries) = 
            (c, t, assemblyWithEmbResourcesQueries);

        public void Deconstruct(out TCon c, out Option<DbTransaction> t) =>
            (c, t) = (Connection, Transaction);
    }


    //Monadic interface 
    public static partial class DB
    {
        #region Return
        public static Queries<TMDBConnector.Unit,TState> NewQueries<TState>() => s => (Prelude.unit, s);
        public static Queries<T,TState> NewQueries<T,TState>() => s => (default(T), s);
        public static Queries<T,TState> ToQueries<T,TState>(this T value) =>
            dbState => (value, dbState);

        public static Queries<T,TState> Then<T, TState>(this Func<TState, DbCommand> commandFactory, Func<DbCommand, T> commandToExecute) =>
            dbState => (commandToExecute(commandFactory(dbState)), dbState);

        //DataAdapters 
        public static Queries<int, TState> ThenDataAdapterFill<TState>(this Func<TState, DbCommand> commandFactory, DataTable dt) =>
            commandFactory.Then(c => ToDASelectCommand(c)).Map(DataAdapter)
            .Map(ExecuteAndDispose(da => da.Fill(dt)));
        public static Queries<int, TState> ThenDataAdapterFill<TState>(this Func<TState, DbCommand> commandFactory, DataSet ds) =>
            commandFactory.Then(c => ToDASelectCommand(c)).Map(DataAdapter)
            .Map(ExecuteAndDispose(da => da.Fill(ds)));

        public static Queries<int, TState> ThenDataAdapterUpdate<TState>(this Func<TState, DbCommand> commandFactory,
            DataTable dt, Dictionary<string, Option<string>> paramToSourceColumn = null) =>
            commandFactory.Then(c => ToDAUpdateCommand(c, paramToSourceColumn)).Map(DataAdapter)
            .Map(ExecuteAndDispose(da => da.Update(dt)));

        public static Queries<int,TState> ThenDataAdapterInsert<TState>(this Func<TState, DbCommand> commandFactory, DataTable dt) =>
            commandFactory.Then(c => ToDAInsertCommand(c)).Map(DataAdapter)
            .Map(ExecuteAndDispose(da => da.Update(dt)));

        public static Queries<int,TState> ThenDataAdapterDelete<TState>(this Func<TState, DbCommand> commandFactory, DataTable dt) =>
            commandFactory.Then(c => ToDADeleteCommand(c)).Map(DataAdapter)
            .Map(ExecuteAndDispose(da => da.Update(dt)));
        #endregion

        #region Bind
        public static Queries<TSelector, TState> FlatMap<T, TSelector, TState>(
            this Queries<T, TState> source,
            Func<T, Queries<TSelector, TState>> selector) =>
            SelectMany(source, selector, (t, tSelector) => tSelector);

        //Error handling bind - Extract 2 layers of monads(Queries and then Option)
        public static Queries<Option<TSelector, TError>, TState> FlatMapWithError<T, TSelector, TError, TState>(
            this Queries<Option<T, TError>, TState> source,
            Func<T, Queries<Option<TSelector, TError>, TState>> selector) =>
                oldState =>
                {
                    var (Value, DbState) = source(oldState);
                    return Value.Match(
                        oldValue => selector(oldValue)(DbState)
                        , error => (Option.None<TSelector, TError>(error), DbState)
                    );
                };

        public static Queries<TResult, TState> SelectMany<T, TSelector, TResult, TState>(
            this Queries<T, TState> source,
            Func<T, Queries<TSelector, TState>> selector,
            Func<T, TSelector, TResult> resultSelector) =>
        oldState =>
        {
            var (value, dbState) = source(oldState);
            var (newValue, newDbState) = selector(value)(dbState);
            return (resultSelector(value, newValue), newDbState); // Output new Queries.
        };

        #endregion

        #region Map/Functor
        public static Queries<TResult, TState> Map<T, TResult, TState>(this Queries<T,TState> source, Func<T, TResult> selector) =>
            Select(source, selector);

        public static Queries<Option<TResult, TError>, TState> MapWithError<T, TResult, TError, TState>(
            this Queries<Option<T, TError>, TState> source, Func<T, TResult> selector) =>
            source.FlatMapWithError<T, TResult, TError, TState>(v =>
                s => (selector(v).Some<TResult, TError>(), s)
            );

        public static Queries<TResult, TState> Select<T, TResult, TState>(this Queries<T, TState> source, Func<T, TResult> selector) =>
                oldState =>
                {
                    (T Value, TState state) = source(oldState);
                    TState newState = state;
                    return (selector(Value), newState); // Output new state.
                };

        //TODO: add error handling functor
        #endregion
        
        #region Execute

        public static T Run<T,TCon>(this Queries<T, DBState<TCon>> queries, string connectionString) 
            where TCon : DbConnection, new() =>
            WithConnection<T,TCon>(connectionString, con =>
                 queries(new DBState<TCon>(con, Option.None<DbTransaction>())).Value
            );
        public static T Run<T,TCon>(this Queries<T, DBState<TCon>> queries, DBConnectionString<TCon> cs) 
            where TCon : DbConnection, new() =>
            WithConnection<T,TCon>(cs.ConnectionString, con =>
                 queries(new DBState<TCon>(con, Option.None<DbTransaction>())).Value
            );

        public static T RunWithTransaction<T, TCon, TCon2>(
            this Queries<T, OneOf<DBState<TCon>, DBState<TCon2>>> queries,
            OneOf<DBConnectionString<TCon>, DBConnectionString<TCon2>> css)
            where TCon : DbConnection, new()
            where TCon2 : DbConnection, new()
            =>
            css.Match(
                c1 => WithTransaction<T, TCon>(c1.ConnectionString, dbState => queries(dbState).Value)
                , c2 => WithTransaction<T, TCon>(c2.ConnectionString, dbState => queries(dbState).Value)
            );

        public static Option<T,TError> RunWithTransaction<T, TError,  TCon, TCon2>(
            this Queries<Option<T,TError>, OneOf<DBState<TCon>, DBState<TCon2>>> queries,
            OneOf<DBConnectionString<TCon>, DBConnectionString<TCon2>> css)
            where TCon : DbConnection, new()
            where TCon2 : DbConnection, new()
            =>
            css.Match(
                c1 => WithTransaction<T,TError, TCon>(c1.ConnectionString, dbState => queries(dbState).Value)
                , c2 => WithTransaction<T, TError, TCon>(c2.ConnectionString, dbState => queries(dbState).Value)
            );

        public static T RunWithTransaction<T,TCon>(this Queries<T, DBState<TCon>> queries, string connectionString)
            where TCon : DbConnection, new() =>
            WithTransaction<T,TCon>(connectionString, dbState =>
                 queries(dbState).Value
            );
        public static T RunWithTransaction<T,TCon>(this Queries<T, DBState<TCon>> queries, DBConnectionString<TCon> cs)
            where TCon : DbConnection, new() =>
            WithTransaction<T,TCon>(cs.ConnectionString, dbState =>
                 queries(dbState).Value
            );

        public static Option<T, TError> RunWithTransaction<T, TError, TCon>(this Queries<Option<T, TError>, DBState<TCon>> queries, DBConnectionString<TCon> cs) 
            where TCon : DbConnection, new() =>
            WithTransaction<T,TError, TCon>(cs.ConnectionString, dbState =>
                 queries(dbState).Value
            );

        public static T Run<T, TCon>(this Queries<T, DBState<DbConnection>> qs, DBConnectionString<TCon> cs)
            where TCon : DbConnection, new()
            => WithConnection<T, TCon>(cs.ConnectionString, con => qs(new DBState<DbConnection> { Connection = con }).Value);

        //Execute with alternatives
        public static T Run<T, TCon, TCon2>(this Queries<T, OneOf<DBState<TCon>, DBState<TCon2>>> qs,
            OneOf<DBConnectionString<TCon>, DBConnectionString<TCon2>> css)
            where TCon : DbConnection, new()
            where TCon2 : DbConnection, new()
            =>
            css.Match(
            cs1 => WithConnection<T, TCon>(cs1.ConnectionString, con => qs(new DBState<TCon> { Connection = con }).Value)
            , cs2 => WithConnection<T, TCon2>(cs2.ConnectionString, con => qs(new DBState<TCon2> { Connection = con }).Value)
            );



        #endregion

        #region State manipulation
        public static Queries<TMDBConnector.Unit, DBState<TCon>> SetTransaction<TCon>(Func<DbConnection, DbTransaction, DbTransaction> handleState) 
            where TCon : DbConnection, new() =>
            oldState =>
            {
                var newTrans = oldState.Transaction.Map(oldT =>
                {
                    var newT = handleState(oldState.Connection, oldT);
                    if (!ReferenceEquals(oldT, newT)) oldT.Dispose();
                    return newT;
                });
                return (Prelude.unit, new DBState<TCon>(oldState.Connection, newTrans));
            };
        #endregion

        public static Queries<Option<T, TError>, TState> Catch<T, TException, TError, TState>
            (this Queries<T, TState> queries, Func<TException, TError> handleException)
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


        public static Queries<T, TState> RunIf<T, TState>(this Queries<T, TState> queries, bool pred) =>
            pred ? queries : s => (default(T), s);
        public static Queries<T, TState> RunIf<T, TState>(this Queries<T, TState> queries, bool pred, T alternativeValue) =>
            pred ? queries : s => (alternativeValue, s);
    }


    //Streaming
    public static partial class DB
    {
        public static IObservable<T> ToObservable<T, TCon>(
            this Queries<IEnumerable<T>, DBState<DbConnection>> qs, 
            DBConnectionString<TCon> cs
            )
            where TCon : DbConnection, new()
            => 
            Observable.Using(() => GetConnection(cs)
            , c =>
                {
                    c.Open();
                    return qs(new DBState<DbConnection>(c, Option.None<DbTransaction>())).Value.ToObservable();
                }
            );
            
        public static IObservable<T> ToObservable<T, TCon>(this Queries<IEnumerable<T>, DBState<TCon>> qs,  string cs) 
            where TCon : DbConnection, new()
            =>
            Observable.Using(() => GetConnection<TCon>(cs)
            , c =>
                {
                    c.Open();
                    return qs(new DBState<TCon>(c, Option.None<DbTransaction>())).Value.ToObservable();
                }
            );
    }


    //Usage Example
    public static partial class DB
    {
        internal static void Workflow()
        {
            //Simple Examples

            //Simple Example 1
            var sqlQuery = Command<SqlConnection>("select '123'").Then(c => (string)c.ExecuteScalar());
            string sqlQueryResult = sqlQuery.Run("connectionString");

            //Simple Example 2
            var bolOfRead =
                Command<HanaConnection>("sfds").Then(c => c.ExecuteReader())
                    .Map(dr => dr.Read())
                    .Run("CS");

            //Queries that contain value(DBProvider wont be used)
            Queries<string, DBState<HanaConnection>> d1 = s => ("", s);

            //Multiple DBProvider options
            var oneOfQueries =
                Command<SqlConnection>("select col1 from t1").Or(Command<HanaConnection>("select \"col1\" from \"t1\""))
                .AddParameters(new DbParameter.NVarChar("p1", "v1"), new DbParameter.BigInt("p2", 2))
                .Then(c => c.ExecuteReader()).Map(Convert.ToDateTime);


            var hanaCon = new DBConnectionString<HanaConnection>("hanaCS");
            var sqlCon = new DBConnectionString<SqlConnection>("sqlCS");

            //Run can be either DBConnectionString 
            DateTime sqlResult = oneOfQueries.Run(sqlCon); 
            DateTime hanaResult = oneOfQueries.Run(hanaCon);

            //or DBConnectionData (child object of DBConnectionString)
            var sqlConData = new DBConnectionData<SqlConnection>("serverName", new ConnectionAuth.UserAndPass("u","p"), "DB");
            DateTime sqlResult2 = oneOfQueries.Run(sqlConData); 


            //This can be a DBConnectionString/DBConnectionData with any connection type. If command from file cannot find an emmbeded query file with the specified name
            //And extension name(based on the connection type name) runtime error gonna be thrown.
            var resultFromQueriesFromFile = CommandFromFile("embededFileNameOfQuery").Then(c => c.ExecuteScalar()).Run(sqlCon);
                

            var resultFromEitherqueryFromFileOrQueryFromCommand = CommandFromFile("embededFileNameOfQuery")
                .Or(
                    //Will run for sql only the rest gonna use CommandFromFile
                    Command<SqlConnection>("select 1")
                ).Then(c => c.ExecuteScalar()).Run(hanaCon) ;

            var str =
                CommandFromFile("file").Then(c => (int)c.ExecuteScalar())
                .FlatMap(i =>
                    Command("select 'im string'").Then(c => (string)c.ExecuteScalar())
                ).Run(new DBConnectionData<SqlConnection>("s1", new ConnectionAuth.UserAndPass("u", "12345"), "db"));


            //LINQ example
            var qs =
                from queryResult in Command<SqlConnection>("query1").Then(c => c.ExecuteScalar()).Map(Convert.ToDouble)
                from queryResult2 in Command<SqlConnection>("query2").Then(c => c.ExecuteScalar()).Map(Convert.ToDateTime)
                //Note that the queries state needs to stay the same throwout the computation so the following wont compile
                //If alternative is needed "Or" should be used before "Then"
                select (queryResult, queryResult2);

            var (r1, r2) = qs.Run("CS");

            //Multiple DBProvider options
            var qs2 =
                from queryResult in Command<SqlConnection>("sQuery1").Or(Command<HanaConnection>("hQuery1"))
                                    .Then(c => c.ExecuteScalar()).Map(Convert.ToDouble)
                from queryResult2 in Command<SqlConnection>("sQuery2").Or(Command<HanaConnection>("hQuery2"))
                                    .Then(c => c.ExecuteScalar()).Map(Convert.ToDouble)
                select (queryResult, queryResult2);


            var (mulDBR1, mulDBR2) = qs2.Run(new DBConnectionString<SqlConnection>("CS"));

            //Transactions
            var (mulDBR3, mulDBR4) = qs2.RunWithTransaction(new DBConnectionString<HanaConnection>("CS"));
            
            //If queries return type is of type Option<T,TError> 
            //if runned in transaction and TError was returned, the transaction will be rolled back
            var qsT =
                from queryResult in Command<SqlConnection>("sQuery1").Or(Command<HanaConnection>("hQuery1"))
                                    .Then(c => c.ExecuteScalar()).Map(Convert.ToDouble)
                from queryResult2 in Command<SqlConnection>("sQuery2").Or(Command<HanaConnection>("hQuery2"))
                                    .Then(c => c.ExecuteScalar()).Map(Convert.ToDouble)
                select Option.None<TMDBConnector.Unit, string>("some error this will rollback the transaction");


            var optionalWillContainErr = qsT.RunWithTransaction(new DBConnectionString<SqlConnection>("CS"));

            



            //Streaming (Reactive Extensions)
            var dbMainStream = CommandFromFile("selectALotOfRowsQuery")
                .Then(EnumerateRows).ToObservable(new DBConnectionString<SqlConnection>("CS"));

            var stopTrigger =
                Observable.Interval(TimeSpan.FromSeconds(2))
                .Select(_ =>
                    CommandFromFile("checkIfTableHasNewRows").Then(c => c.ExecuteScalar()).Map(Convert.ToInt32)
                    .Run(hanaCon)
                );

            var bigQueryWithRefresh = dbMainStream.WithLatestFrom(stopTrigger, (d,i)=> (d:d,i:i))
                .TakeWhile(t=>(int)t.d["c1"]<t.i).Select(t=> t.d) ;

            //bigQueryWithRefresh.Subscribe(row => Console.WriteLine(row["c2"]));
            stopTrigger.Subscribe(row => Console.WriteLine(row.ToString()));

        }
    }

}
