# DBMonad - SQL and HANA

## Impertive to Functional

This library was made to make the .net DB Client(DBConnection , DBCommand...) interface more reusable by providing monadic API.
Moving from imprative interface to more functional one.
All the main functionality is build in static methods

## Unifing DB providers - less verbosity

The DB Providers are not exposed(except of then method which expose the DBCommand in order to chose quering method)
This reduce the verbosity that exists in the current DB Client's interface.

## Reusablity

the Queries<T> type "holds" a db compution that can be composed from other Queries(with the parameter) throfor it can be stored in a variable/returned from a method without actually evaluating the experions.
  
## Transactions

to make a db compution(Queries<T>) to be in transaction just trigger RunWithTransaction instead of run.

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
```
dbConputions.Run(ServerPlatform.Hana, "connection string");
