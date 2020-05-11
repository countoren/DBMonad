# DBMonad

## Imperative to Functional

This library is a thin wrapper over the .net DB Providers(DBConnection, DBCommand...) it was made to make the interface more functional and have more compile-time guarantees.
All the main functionality is built in static methods and the state is managed(hidden) "behind" a monad type which represents the DB queries.

## Less verbosity

```Queries``` type provides a run-in connection context therefor some of the DBProvider's methods can be hidden(open, close, dispose, transactions handling...).
DBMonad contains extensions methods that help with having a more fluent interface and the entire DB integration can be written with expressions only. 

## Reusability

the ```Queries<T,DbState<TCon>>``` type "holds" a DB computation that can be composed of other Queries(with their DB parameters) therefore it can be stored in a variable or returned from a method without actually evaluating the expressions.
* T - represents a return type from the queries
* ```DBState<TCon>``` - is a DB state of TCon type (like SqlConnection) that contains properties like (like Connection, transaction...) 

## Installation

The package can be download from [Nuget](https://www.nuget.org/packages/DBMonad) either from  the GUI or the NuGet CLI tool with:
```Install-Package DBMonad```

## Simple Examples:

__.Net DbProvider__ interface simple SqlConnection based on [Microsoft SqlConnection Example](https://docs.microsoft.com/en-us/dotnet/api/microsoft.data.sqlclient.sqlconnection?view=sqlclient-dotnet-core-1.1): 
```CS
  using(var connection = new SQLConnection("sql connection string"))
  {
     connection.Open();
     using (var dbCommand = new SQLCommand("some sql query", connection))
     {
        var preProcessedResult = dbCommand.ExecuteScalar();
        var result = Convert.ToDateTime(preProcessedResult);
     }
  }
```

__DB Monad__ interface simple SqlConnection:
```CS
  var result =  Command<SqlConnection>("some sql query")
	  .Then(c => c.ExecuteScalar()).Map(Convert.ToDateTime).Run("ConnectionString")
```

## More complex example with transaction and Alternatives of DBProviders:

__.Net DbProvider__ interface:

```cs
If(isInSqlConfiguration)
{ 
    using (SqlConnection connection = new SqlConnection(connectionString))
    {
        connection.Open();

        SqlCommand command = connection.CreateCommand();
        SqlTransaction transaction;

        // Start a local transaction.
        transaction = connection.BeginTransaction("SampleTransaction");

        // Must assign both transaction object and connection
        // to Command object for a pending local transaction
        command.Connection = connection;
        command.Transaction = transaction;

        try
        {
            command.CommandText = "1st insert query sql";
            command.ExecuteNonQuery();
            command.CommandText ="2nd insert query hana";
            command.ExecuteNonQuery();

            // Attempt to commit the transaction.
            transaction.Commit();
        }
        catch (Exception ex)
        {
            transaction.Rollback();
        }
    }
}
If(isInHanaConfiguration)
{ 
    using (HanaConnection connection = new HanaConnection(connectionString))
    {
        connection.Open();

        HanaCommand command = connection.CreateCommand();
        HanaTransaction transaction;

        // Start a local transaction.
        transaction = connection.BeginTransaction("SampleTransaction");

        // Must assign both transaction object and connection
        // to Command object for a pending local transaction
        command.Connection = connection;
        command.Transaction = transaction;

        try
        {
            command.CommandText = "1st insert query hana";
            command.ExecuteNonQuery();
            command.CommandText = "2nd insert query hana";
            command.ExecuteNonQuery();

            // Attempt to commit the transaction.
            transaction.Commit();
        }
        catch (Exception ex)
        {
            transaction.Rollback();
        }
    }
}
```

__DB Monad__ interface:
```cs
Command<SqlConnection>("1st insert query sql").Or(Command<HanaConnection>("1st insert query hana"))
.Then(c=> c.ExecuteNonQuery())
.FlatMap(_=>
   Command<SqlConnection>("2nd insert query sql").Or(Command<HanaConnection>("2nd insert query hana"))
   .Then(c=> c.ExecuteNonQuery())
).RunWithTransaction(new DBConnectionString<HanaConnection>("connection string"))
```

## LINQ

The query value can be "extracted" with from in linq syntax(do blocks in haskell):
```cs
var qs2 =
	from queryResult in 
	   Command<SqlConnection>("select 3.2").Or(Command<HanaConnection>("select 4.3 from dummy"))
           .Then(c => c.ExecuteScalar()).Map(Convert.ToDecimal)
	from queryResult2 in 
	   Command<SqlConnection>("select 6.1").Or(Command<HanaConnection>("select 7.4 from dummy"))
           .Then(c => c.ExecuteScalar()).Map(Convert.ToDecimal)
	select (queryResult, queryResult2);


var (mulDBR1, mulDBR2) = qs2.Run(new DBConnectionString<SqlConnection>(sqlCS));

//Transactions
var (mulDBR3, mulDBR4) = qs2.RunWithTransaction(new DBConnectionString<HanaConnection>(hanaCS));

```
