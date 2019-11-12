using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq;
using System.Reactive.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using DBMonad;
using Optional;
using Sap.Data.Hana;
using static DBMonad.DB;

namespace DBMonad.Tests
{
    class Program
    {
        static void Main(string[] args)
        {

            var sqlCon = new DBConnectionData<SqlConnection>(
                "SQLServer"
                , new ConnectionAuth.UserAndPass("u", "p")
                , "DBSql"
            );

            var sqlCS = sqlCon.ToString();

            var hanaCon = new DBConnectionData<HanaConnection>(
                "hanaServer"
                , new ConnectionAuth.UserAndPass("u", "p")
            );

            var hanaCS = hanaCon.ToString();



            //Simple Examples

            //Simple Example 1
            var sqlQuery = Command<SqlConnection>("select '123'").Then(c => (string)c.ExecuteScalar());
            string sqlQueryResult = sqlQuery.Run(sqlCS);



            //Simple Example 2 - with specific provider's DbParameters(HanaParameter in this case)
            var bolOfRead =
                Command<HanaConnection>("select :p1 from dummy",
                    new HanaParameter { ParameterName = "p1", HanaDbType = HanaDbType.NVarChar, Value = "123" }
                ).Then(c => c.ExecuteReader())
                .ReadRow().Map(mr => mr.Map(r => r.First().Value.ToString()).ValueOr("NoValue"))
                .Run(hanaCS);

            //Queries that contain value(DBProvider wont be used)
            Queries<string, DBState<HanaConnection>> d1 = s => ("", s);

            //Multiple DBProvider options
            var oneOfQueries =
                Command<SqlConnection>("select @pSQL1"
                //This SqlParameter will be given only to the SqlCommand
                , new SqlParameter { ParameterName = "@pSQL1", DbType = System.Data.DbType.Int32, Value = 1 }
                ).Or(Command<HanaConnection>("select :p1 from dummy"))
                //DBMonad DbParameters can be supplied those parameters will be given to all the DbProviers commands options
                .AddParameters(new DbParameter.BigInt("p1", 2))
                .Then(c => c.ExecuteScalar()).Map(Convert.ToInt32);

            //Run DBConnectionData (child object of DBConnectionString)
            int sqlResult = oneOfQueries.Run(sqlCon);
            int hanaResult = oneOfQueries.Run(hanaCon);

            //or can be either DBConnectionString 
            var sqlConStr = new DBConnectionString<SqlConnection>(sqlCS);
            int sqlResult2 = oneOfQueries.Run(sqlConStr);


            //This can be a DBConnectionString/DBConnectionData with any connection type. If command from file cannot find an emmbeded query file with the specified name
            //And extension name(based on the connection type name) runtime error gonna be thrown.
            var resultFromQueriesFromFile = CommandFromFile("exampleQuery").Then(c => c.ExecuteScalar()).Run(sqlCon);


            var EitherqueryFromFileOrQueryFromCommand =
                    //Will run for sql only the rest gonna use CommandFromFile
                    Command<SqlConnection>("select 'inlined query'")
                .Or(
                    CommandFromFile("exampleQuery")
                ).Then(c => c.ExecuteScalar());

            var resultFromEmbedded = EitherqueryFromFileOrQueryFromCommand.Run(sqlCon);
            var resultFromCommand = EitherqueryFromFileOrQueryFromCommand.Run(hanaCon);

            var str =
                CommandFromFile("exampleQuery").Then(c => c.ExecuteScalar()).Map(Convert.ToString)
                .FlatMap(i =>
                    Command("select 'im string'").Then(c => (string)c.ExecuteScalar())
                ).Run(new DBConnectionString<SqlConnection>(sqlCS));


            //LINQ example
            var qs =
                from queryResult in Command<SqlConnection>("select 3.2").Then(c => c.ExecuteScalar()).Map(Convert.ToDouble)
                from queryResult2 in Command<SqlConnection>("select GetDate()").Then(c => c.ExecuteScalar()).Map(Convert.ToDateTime)
                //Note that the queries state needs to stay the same throwout the computation so the following wont compile
                //If alternative is needed "Or" should be used before "Then"
                select (queryResult, queryResult2);

            var (r1, r2) = qs.Run(sqlCS);

            //Multiple DBProvider options
            var qs2 =
                from queryResult in Command<SqlConnection>("select 3.2").Or(Command<HanaConnection>("select 4.3 from dummy"))
                                    .Then(c => c.ExecuteScalar()).Map(Convert.ToDecimal)
                from queryResult2 in Command<SqlConnection>("select 6.1").Or(Command<HanaConnection>("select 7.4 from dummy"))
                                    .Then(c => c.ExecuteScalar()).Map(Convert.ToDecimal)
                select (queryResult, queryResult2);


            var (mulDBR1, mulDBR2) = qs2.Run(new DBConnectionString<SqlConnection>(sqlCS));

            //Transactions
            var (mulDBR3, mulDBR4) = qs2.RunWithTransaction(new DBConnectionString<HanaConnection>(hanaCS));
            
            //If queries return type is of type Option<T,TError> 
            //if runned in transaction and TError was returned, the transaction will be rolled back
            var qsT =
                from queryResult in Command<SqlConnection>("select 3.2").Or(Command<HanaConnection>("select 4.3 from dummy"))
                                    .Then(c => c.ExecuteScalar()).Map(Convert.ToDecimal)
                from queryResult2 in Command<SqlConnection>("select 6.1").Or(Command<HanaConnection>("select 7.4 from dummy"))
                                    .Then(c => c.ExecuteScalar()).Map(Convert.ToDecimal)
                select Option.None<TMDBConnector.Unit, string>("some error this will rollback the transaction");


            var optionalWillContainErr = qsT.RunWithTransaction(sqlCon);


            //Streaming (Reactive Extensions)
            var dbMainStream = CommandFromFile("selectALotOfRowsQuery")
                .Then(DB.EnumerateRows).ToObservable(sqlCon);

            var stopTrigger =
                Observable.Interval(TimeSpan.FromSeconds(2))
                .Select(_ =>
                    CommandFromFile("checkIfTableHasNewRows").Then(c => c.ExecuteScalar()).Map(Convert.ToInt32)
                    .Run(hanaCon)
                );

            var bigQueryWithRefresh = dbMainStream.WithLatestFrom(stopTrigger, (d,i)=> (d:d,i:i))
                .TakeWhile(t=>(int)t.d["c1"]<t.i).Select(t=> t.d) ;

            stopTrigger.Subscribe(row => Console.WriteLine(row.ToString()));


            Console.ReadKey();



        }

    }
}
