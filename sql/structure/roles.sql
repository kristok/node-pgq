DO
$$
BEGIN
   IF NOT EXISTS (
      SELECT *
        FROM pg_catalog.pg_user
       WHERE usename = 'pgq_reader') THEN
          CREATE ROLE pgq_reader;
   END IF;

   IF NOT EXISTS (
      SELECT *
        FROM pg_catalog.pg_user
       WHERE usename = 'pgq_writer') THEN
          CREATE ROLE pgq_writer;
   END IF;

   IF NOT EXISTS (
      SELECT *
        FROM pg_catalog.pg_user
       WHERE usename = 'pgq_admin') THEN
          CREATE ROLE pgq_admin;
   END IF;
END
$$;