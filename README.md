# DBMonad - SQL and HANA

## Imperative to Functional

This library was made to make the .net DB Client(DBConnection , DBCommand...) interface more reusable by providing monadic API.
Moving from imperative interface to more functional one.
All the main functionality is built in static methods

## Unifying DB providers - less verbosity

The DB Providers are not exposed (except "Then" method which exposes the DBCommand to choose the quering method)
therefore reduce the verbosity that exists in the current DB Client's interface.

## Reusability

the Queries<T> type "holds" a DB computation that can be composed of other Queries(with their DB parameters) therefore it can be stored in a variable or returned from a method without actually evaluating the expressions.
  
## Transactions

to make a DB computation(Queries<T>) to be in a transaction trigger RunWithTransaction instead of run.

## Simple Example:

from this:
```
DBConnection connection;
if(isInSqlConfiguration)
  connection = new SQLConnection("sql connection string");
if(isInHanaConfiguration)
  connection = new HanaCommand("hana connection string");

DBCommand dbCommand;
if(isInSqlConfiguration)
  dbCommand = new SQLCommand("some sql query", connection);
if(isInHanaConfiguration)
  dbCommand = new HanaCommand("some hana query", connection);
  
var result = dbCommand.ExecuteScalar();
```

To this:
```
var result = DB.command("some sql query", "some hana query")
.Then(c=> c.ExecuteScalar())
.Run(ServerPlatform.Hana, "connection string")
```

## LINQ

The query value can be "extracted" with from in linq syntax(do blocks in haskell):
```
var dbComputions = 
from firstString in DB.command("some sql query", "some hana query")
                    .then(c=> c.ExecuteScalar()).Map(Convert.ToString)
from secondString in DB.command("some sql query2", "some hana query2")
                    .then(c=> c.ExecuteScalar()).Map(Convert.ToString)
select $"{firstString} , {secondString}";

dbComputions.Run(ServerPlatform.Hana, "connection string");
```

## Notes

At the moment the library has support for only SQL and Hana providers, but it should be pretty easy to add more.
