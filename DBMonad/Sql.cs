using OneOf;
using Optional;
using Sap.Data.Hana;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DBMonad
{
    public static class Sql
    {
        public delegate (T value, TState state) Queries<T, TState>(TState s);

        public class DBState<C> where C : DbConnection
        {
            public C Connection;
            public Option<DbTransaction> Transaction;
        }

        public class DBConnectionString<TCon> where TCon :DbConnection
        {
            public string cs;
        }


        public static Func<DBState<TCon>, DbCommand> Command<TCon>(string query, params System.Data.Common.DbParameter[] ps) 
            where TCon : DbConnection => 
            s => Command(s.Connection, s.Transaction, query, ps);

        public static Func<DBState<DbConnection>, DbCommand> Command(string query, params System.Data.Common.DbParameter[] ps) =>
            Command<DbConnection>(query, ps);

        //public static Func<DBState,DbCommand> CommandFromF(string fileName)
        //{
        //    var q = "";

            
        //}

        public static DbCommand Command(DbConnection connection, Option<DbTransaction> transaction, string query, params System.Data.Common.DbParameter[] ps)
        {
                var cmd = connection.CreateCommand();
                cmd.CommandText = query;
                transaction.MatchSome(t => cmd.Transaction = t);
                cmd.Parameters.AddRange(ps);
                return cmd;
        }

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


        public static Queries<T,TState> Then<T,TState>(
            this Func<TState, DbCommand> commandFactory, 
            Func<DbCommand, T> handler
            ) => s => (handler(commandFactory(s)), s);


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



        public static T Run<T, TCon>(this Queries<T, DBState<TCon>> qs, string cs)
            where TCon : DbConnection, new()
            => WithConnection<T, TCon>(cs, con => qs(new DBState<TCon> { Connection = con }).value);

        public static T Run<T, TCon>(this Queries<T, DBState<DbConnection>> qs, DBConnectionString<TCon> cs)
            where TCon : DbConnection, new()
            => WithConnection<T, TCon>(cs.cs, con => qs(new DBState<DbConnection> { Connection = con }).value);


        public static T Run<T, TCon, TCon2>(this Queries<T, OneOf<DBState<TCon>, DBState<TCon2>>> qs,
            OneOf<DBConnectionString<TCon>, DBConnectionString<TCon2>> css)
            where TCon : DbConnection, new()
            where TCon2 : DbConnection, new()
            =>
            css.Match(
            cs1 =>
                WithConnection<T, TCon>(cs1.cs, con => qs(new DBState<TCon> { Connection = con }).value)
            , cs2 =>
                WithConnection<T, TCon2>(cs2.cs, con => qs(new DBState<TCon2> { Connection = con }).value)
            );
            



        public static T WithConnection<T, TCon>(string connectionString, Func<TCon, T> inOpenConnection)
            where TCon : DbConnection, new()
        {
            using (var con = GetConnection<TCon>(connectionString))
            {
                con.Open();
                return inOpenConnection(con);
            }
           
        }
        public static TCon GetConnection<TCon>(string connectionString) where TCon : DbConnection, new() => 
            new TCon { ConnectionString = connectionString };
    }


    public static class test
    {


        public static void main()
        {

            var b = 
            Sql.Command("select 123 ")
                .Or(Sql.Command<SqlConnection>("dsfsdfsdf")).Then(c => c.ExecuteScalar())
                .Run(new Sql.DBConnectionString<SqlConnection> { cs = "" });

            Sql.Command<SqlConnection>("wefds").Then(c => c.ExecuteScalar()).Run("ff");
            //Sql.Command("xczx", new HanaParameter()).Then(c=> c.ExecuteScalar()).Map(Convert.ToString)
            OneOf<string, int> v = "dsfsdf";
            OneOf<string, int> v2 = 2;


            var a =
            Sql.Command<SqlConnection>("wefds")
            .Or(Sql.Command<HanaConnection>("fsfdsf"))
            .AddParameters(new DbParameter.NVarChar("dafsdf"))
            
            .Then(c=>(string) c.ExecuteScalar())
            .Run(new Sql.DBConnectionString<SqlConnection> { cs = ""});
        }

    }
}
