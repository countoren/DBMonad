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

.Net DbProvider interface simple SqlConnection: 
```
  var connection = new SQLConnection("sql connection string");
  var dbCommand = new SQLCommand("some sql query", connection);
  var preProcessedResult = dbCommand.ExecuteScalar();
  var result = Convert.ToDateTime(preProcessedResult);
```

DB Monad interface
```
  var result =  Command<SqlConnection>("some sql query")
	  .Then(c=> c.ExecuteScalar()).Map(Convert.ToDateTime).Run("CS")
```


.Net DB Client interface:
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

DB Monad interface:
```
var result = Command<SqlConnection>("some sql query").Or(Command<HanaConnection>("some hana query"))
.Then(c=> c.ExecuteScalar())
.Run(new DBConnectionString<SqlConnection>("connection string"))
```

## LINQ

The query value can be "extracted" with from in linq syntax(do blocks in haskell):
```
var qs2 =
	from queryResult in Command<SqlConnection>("select 3.2").Or(Command<HanaConnection>("select 4.3 from dummy"))
						.Then(c => c.ExecuteScalar()).Map(Convert.ToDecimal)
	from queryResult2 in Command<SqlConnection>("select 6.1").Or(Command<HanaConnection>("select 7.4 from dummy"))
						.Then(c => c.ExecuteScalar()).Map(Convert.ToDecimal)
	select (queryResult, queryResult2);


var (mulDBR1, mulDBR2) = qs2.Run(new DBConnectionString<SqlConnection>(sqlCS));

//Transactions
var (mulDBR3, mulDBR4) = qs2.RunWithTransaction(new DBConnectionString<HanaConnection>(hanaCS));

```
