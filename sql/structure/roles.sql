DO
$$
BEGIN
   IF NOT EXISTS (
      SELECT *
        FROM pg_catalog.pg_roles
       WHERE rolname = 'pgq_reader') THEN
          CREATE ROLE pgq_reader;
   END IF;

   IF NOT EXISTS (
      SELECT *
        FROM pg_catalog.pg_roles
       WHERE rolname = 'pgq_writer') THEN
          CREATE ROLE pgq_writer;
   END IF;

   IF NOT EXISTS (
      SELECT *
        FROM pg_catalog.pg_roles
       WHERE rolname = 'pgq_admin') THEN
          CREATE ROLE pgq_admin;
   END IF;
END
$$;