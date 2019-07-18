using System;
using System.Collections.Generic;
using System.Linq;
using System.Reactive.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using DBMonad;

namespace DBMonad.Tests
{
    class Program
    {
        static void Main(string[] args)
        {

            var connection = new DBConnectionData(ServerPlatform.Sql
                , "HOST"
                , new ConnectionAuth.UserAndPass("USER", "PASS")
                , "DB"
            );

            //Streaming 
            var dbMainStream = DB.Command("select *, (select max(c1) from test1)[max] from test1 WITH(NOLOCK)",null)
                .Then(DB.EnumerateRows).ToObservable(connection);

            var stopTrigger =
                Observable.Interval(TimeSpan.FromSeconds(2))
                .Select(_ =>
                    DB.Command("select max(c1) from test1 WITH(NOLOCK)", null).Then(c => c.ExecuteScalar()).Map(Convert.ToInt32)
                    .Run(connection)
                ).StartWith(-1);

            var bigQueryWithRefresh = dbMainStream.WithLatestFrom(stopTrigger, (d, i) => (d: d, i: i))
                .TakeWhile(t => t.i == -1 || (int)t.d["max"] == t.i).Select(t => t.d)
                .Repeat();

            bigQueryWithRefresh.Subscribe(row =>
            {
                Console.WriteLine(row["c2"]);
                Thread.Sleep(1000);
            });


            Console.ReadKey();
        }

    }
}
