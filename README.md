#node-pgq

SkyTools PgQ messaging broker consumer / ticker implementation for Node.js

Summary
-----------
PgQ is the messaging broker implementation for PostgreSQL databases built by Skype engineers. It's the core layer that delivers data in the replication tool Londiste.  PgQ is the perfect messaging implementation if you care that your data never gets lost but don't care about millisecond latency for message delivery or fancy routing. Key reasons why to choose this package are:

 - PgQ core code has run in live for years without causing data loss, message data is as reliable as your database 
 - Messages get created within the same transaction that updates your business data so they won't get out of sync with it
 - Handles massive loads really well
 - You can create your own message formats
 - No limits on message consumers or producers
 - No additional technology stack needed, some tables, functions & triggers are added into your database. Everything else can be controlled from Node.js

The original PgQ implementation is written in C, python & PL/pgSQL and available as part of the Skytools package here: https://github.com/markokr/skytools 
