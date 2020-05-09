import 'dart:io';
import 'dart:convert';

import 'package:postgres/postgres.dart';

void main(List<String> arguments) async {
  final conn = PostgreSQLConnection(
    'localhost',
    5435,
    'dart_test',
    username: 'postgres',
    password: 'password',
  );
  await conn.open();

  print('Connected to Postgres database...');

  // NOTE Do this as part of DB setup not in application code!
  // await conn.query('''
  // CREATE TABLE customers(
  //   id serial primary key not null,
  //   name text,
  //   email text,
  //   address text,
  //   country text
  // )
  // ''');

  // Create data
  await conn.query('''
    INSERT INTO customers (name,email,address,country)
    VALUES ('Jermaine Oppong','jermaine@oppong.co','1212 Some Street','United Kingdom')
  ''');

  // Read data
  var results = await conn.query('SELECT * from customers');
  print(results);

  for (var row in results) {
    print('''
    ===
    id: ${row[0]}
    name: ${row[1]}
    email: ${row[2]}
    address: ${row[3]}
    country: ${row[4]}
    ===
    ''');
  }

  // Update data
  await conn.query("UPDATE customers SET country='Ghana' WHERE id=1");

  // Delete data
  await conn.query('DELETE FROM customers WHERE id > 0');

  // Populate customers table
  await conn.transaction((ctx) async {
    final mockStr = await File('./mock_customers.json').readAsString();
    final mockData = json.decode(mockStr);
    final mockDataStream = Stream.fromIterable(mockData);

    await for (var row in mockDataStream) {
      await ctx.query('''
        INSERT INTO customers (name,email,address,country)
        VALUES (@name,@email,@address,@country)
      ''', substitutionValues: {
        'name': row['name'],
        'email': row['email'],
        'address': row['address'],
        'country': row['country'],
      });
    }
  });

  // NOTE Do this as part of DB setup not in application code!
  // await conn.query('''
  // CREATE TABLE orders(
  //   id serial primary key not null,
  //   order_id int not null,
  //   customer_id int not null,
  //   order_date date
  // )
  // ''');

  // Populate orders table
  await conn.transaction((ctx) async {
    final mockStr = await File('./mock_orders.json').readAsString();
    final mockData = json.decode(mockStr);
    final mockDataStream = Stream.fromIterable(mockData);

    await for (var row in mockDataStream) {
      await ctx.query('''
        INSERT INTO orders (order_id,customer_id,order_date)
        VALUES (@orderId,@customerId,@orderDate)
      ''', substitutionValues: {
        'orderId': row['order_id'],
        'customerId': row['customer_id'],
        'orderDate': row['order_date'],
      });
    }
  });

  // Mapped results of customers that have an order
  var resultsMap = await conn.mappedResultsQuery('''
    SELECT customers.name, orders.order_id
    FROM customers
    LEFT JOIN orders ON customers.id = orders.customer_id
    WHERE orders.order_id > 0
    ORDER BY customers.name
  ''');
  // print(resultsMap);

  for (var row in resultsMap) {
    print('''
    ===
    Customer name: ${row['customers']['name']}
    Order Id: ${row['orders']['order_id']}
    ===
    ''');
  }

  await conn.close();
}
