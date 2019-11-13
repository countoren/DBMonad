# DBMonad

## Imperative to Functional

This library was made to make the .net DB Client(DBConnection , DBCommand...) interface more reusable by providing higher level monadic interface.
Moving from imperative interface to more functional one.
All the main functionality is built in static methods.

## Unifying DB providers - less verbosity

The DB Providers are not exposed (except "Then" method which exposes the DBCommand to choose the quering method)
therefore reduce the verbosity that exists in the current DB Client's interface.

## Reusability

the ```Queries<T,DbState<TCon>>``` type "holds" a DB computation that can be composed of other Queries(with their DB parameters) therefore it can be stored in a variable or returned from a method without actually evaluating the expressions.
* T - represnt a return type from the queries
* ```DBState<TCon>``` - is a DB state of TCon type (like SqlConnection) that contains properties like (like Connection, transaction...) 
  
## Simple Example:

.Net DbProvider interface simple SqlConnection: 
```
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

DB Monad interface simple SqlConnection:
```
  var result =  Command<SqlConnection>("some sql query")
	  .Then(c => c.ExecuteScalar()).Map(Convert.ToDateTime).Run("CS")
```

## More complex example with transaction and Alternatives of DBProviders:

.Net DbProvider interface:

```
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

DB Monad interface:
```
Command<SqlConnection>("1st insert query sql").Or(Command<HanaConnection>("1st insert query hana"))
.Then(c=> c.ExecuteNonQuery())
.FlatMap(_=>
   Command<SqlConnection>("2nd insert query sql").Or(Command<HanaConnection>("2nd insert query hana"))
   .Then(c=> c.ExecuteNonQuery())
).RunWithTransaction(new DBConnectionString<HanaConnection>("connection string"))
```

## LINQ

The query value can be "extracted" with from in linq syntax(do blocks in haskell):
```
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
