CREATE OR REPLACE FUNCTION pgq.trigger_log_json() RETURNS TRIGGER AS $$
-- ----------------------------------------------------------------------
-- Function: pgq.logutriga()
--
--      Trigger function that puts row data JSON encoded into queue.
--
-- Trigger parameters:
--      arg1 - queue name
--      arg2 - optionally 'SKIP'
--
-- Queue event fields:
--   ev_type      - I/U/D
--   ev_data      - columns and values of new row in JSON
--   ev_extra1    - columns and values of old row in JSON
--   ev_extra1    - table name
--   ev_extra2    - primary key columns of table (comma separated)
--
-- Regular listen trigger example:
-- >  CREATE TRIGGER trg_name AFTER INSERT OR UPDATE ON customer
-- >  FOR EACH ROW EXECUTE PROCEDURE pgq.log_trigger_json('queue_name');
--
-- Trigger that sends events to queue without inserting data to table:
-- >   CREATE TRIGGER trg_name AFTER INSERT OR UPDATE ON customer
-- >   FOR EACH ROW EXECUTE PROCEDURE pgq.log_trigger_json('queue_name', 'SKIP');
-- ----------------------------------------------------------------------
DECLARE
	queue text;
	skip boolean;
	primary_key_columns text;
	table_name text;
	_res record;
BEGIN

	-- validate that arguments defined during trigger creation are correct
	IF TG_NARGS = 1 OR TG_NARGS = 2 THEN
		queue = TG_ARGV[0];
		skip = TG_ARGV[1] = 'skip' OR TG_ARGV[1] = 'SKIP';
	ELSE
		-- if trigger has incorrect parameters then warn but don't throw
		RAISE WARNING 'trigger_log_json, incorrect parameters';
		RETURN NEW;
	END IF;

	-- find full table name (trigger data has it without schema)
	SELECT n.nspname || '.' || c.relname
	  FROM pg_namespace n,
	       pg_class c
	 WHERE n.oid = c.relnamespace
	   AND c.oid = TG_RELID
	  INTO table_name;

	-- find primary key columns of the table the trigger is attached to
	SELECT string_agg(a.attname, ',')
	  FROM pg_index i,
	       pg_attribute a
	 WHERE i.indrelid = TG_RELID
	   AND a.attrelid = i.indexrelid
	   AND i.indisprimary
	   AND a.attnum > 0
	   AND NOT a.attisdropped
	  INTO primary_key_columns;

	-- insert event into the queue
	PERFORM * FROM pgq.insert_event(
		queue::text,
		TG_OP::text,
		row_to_json(NEW)::text,
		row_to_json(OLD)::text,
		table_name::text,
		primary_key_columns::text,
		NULL::text
	);

	-- if skip is set we don't write to the table, just throw away the data
	IF skip THEN
		RETURN NULL;
	ELSE
		RETURN NEW;
	END IF;
END;
$$
LANGUAGE plpgsql;

