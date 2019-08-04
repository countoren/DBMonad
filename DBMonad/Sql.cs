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
        public delegate (T value, DBState<TCon> state) Queries<T, TCon>(DBState<TCon> s)
            where TCon : DbConnection;

        public delegate (T value, OneOf<DBState<TCon>, DBState<TCon2>> state) Queries<T, TCon, TCon2>(OneOf<DBState<TCon>, DBState<TCon2>> s)
            where TCon : DbConnection
            where TCon2 : DbConnection;
        public class DBState<C> where C : DbConnection
        {
            public C Connection;
            public Option<DbTransaction> Transaction;
        }

        public class DBConnectionString<TCon> where TCon :DbConnection
        {
            public string cs;
        }

        //public static Func<HanaConnection, HanaTransaction, HanaCommand> HanaCommand(string query, params HanaParameter[] ps) =>
        //    (c,t) => Command<HanaConnection, HanaTransaction, HanaParameter, HanaCommand>(c, t, query, ps);
        //public static Func<SqlConnection, SqlTransaction, SqlCommand> SqlCommand(string query, params SqlParameter[] ps) =>
        //    (c,t) => Command<SqlConnection, SqlTransaction, SqlParameter, SqlCommand>(c, t, query, ps);

        //public static Func<TCon, TTrans, TCom> Command<TCon, TTrans, TParam, TCom>(string query, params TParam[] ps)
        //    where TCon : DbConnection
        //    where TTrans : DbTransaction
        //    where TParam : System.Data.Common.DbParameter
        //    where TCom : DbCommand, new()
        //{
        //}

        public static Func<DBState<TCon>, DbCommand> Command<TCon>(string q, params DbParameter[] ps)
            where TCon : DbConnection
            => s => {
                var cmd = s.Connection.CreateCommand();
                cmd.CommandText = q;

                s.Transaction.MatchSome(t => cmd.Transaction = t);
                cmd.Parameters.AddRange(ps);
                return cmd;
            };
        public static Func<OneOf<DBState<TCon>, DBState<TCon2>>, DbCommand> Or<TCon, TCon2>(
            this Func<DBState<TCon>, DbCommand> commandFactory, 
            Func<DBState<TCon2>, DbCommand> commandFactory2
            ) where TCon : DbConnection
            where TCon2 : DbConnection
            => ss => ss.Match(commandFactory, commandFactory2);

        public static Queries<T,TCon> Then<T,TCon>(
            this Func<DBState<TCon>, DbCommand> commandFactory, 
            Func<DbCommand, T> handler
            ) where TCon : DbConnection
            => s => (handler(commandFactory(s)), s);

        public static Queries<T,TCon, TCon2> Then<T,TCon, TCon2>(
            this Func<OneOf<DBState<TCon>, DBState<TCon2>>, DbCommand> commandsFactory, 
            Func<DbCommand, T> handler
            ) where TCon : DbConnection
             where TCon2 : DbConnection
            => s => (handler(commandsFactory(s)), s);
        public static T Run<T, TCon>(this Queries<T, TCon> qs, string cs)
            where TCon : DbConnection, new()
            => WithConnection<T, TCon>(cs, con => qs(new DBState<TCon> { Connection = con }).value);

        public static T Run<T, TCon, TCon2>(this Queries<T, TCon, TCon2> qs,
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

            Sql.Command<SqlConnection>("wefds").Then(c => c.ExecuteScalar()).Run("ff");
            //Sql.Command("xczx", new HanaParameter()).Then(c=> c.ExecuteScalar()).Map(Convert.ToString)

            var a =
            Sql.Command<SqlConnection>("wefds")
            .Or(Sql.Command<HanaConnection>("fsfdsf"))
            
            .Then(c=> c.ExecuteScalar())
            .Run(new Sql.DBConnectionString<SqlConnection> { cs = ""});
        }

    }
}
