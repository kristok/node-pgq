CREATE OR REPLACE FUNCTION pgq.insert_event_raw (
	queue_name text,
	ev_id integer,
	ev_time timestamptz,
	ev_owner integer,
	ev_retry integer,
	ev_type text,
	ev_data text,
	ev_extra1 text,
	ev_extra2 text,
	ev_extra3 text,
	ev_extra4 text
) RETURNS bigint AS $$
#variable_conflict use_variable
-- ----------------------------------------------------------------------
-- Function: pgq.insert_event_raw(11)
--
--    Pure pl/PgSQL version of pgq.insert_event_raw
--    the C version is definitely faster but can't be installed without
--    access to the database server. This works also via SQL
--
--    Actual event insertion.  Used also by retry queue maintenance.
--
-- Parameters:
--      queue_name      - Name of the queue
--      ev_id           - Event ID.  If NULL, will be taken from seq.
--      ev_time         - Event creation time.
--      ev_owner        - Subscription ID when retry event. If NULL, the event is for everybody.
--      ev_retry        - Retry count. NULL for first-time events.
--      ev_type         - user data
--      ev_data         - user data
--      ev_extra1       - user data
--      ev_extra2       - user data
--      ev_extra3       - user data
--      ev_extra4       - user data
--
-- Returns:
--      Event ID.
-- ----------------------------------------------------------------------
DECLARE
	event_table record;
	_ev_id bigint;
	query text;
BEGIN

	-- find current event table for the queue we are inserting event into
	SELECT queue_data_pfx||'_'||queue_cur_table as name,
	       -- bump seq even if id is given
	       nextval(queue_event_seq) as next_id
	  FROM pgq.queue
	 WHERE queue_name = queue_name
	  INTO event_table;

	IF NOT FOUND THEN
		RAISE EXCEPTION 'Unknown event queue: %', queue_name;
	END IF;

	IF ev_id IS NULL THEN
		_ev_id = event_table.next_id;
	ELSE
		_ev_id = ev_id;
	END IF;

	query = $query$
		INSERT INTO %s (
		    ev_id,
		    ev_time,
		    ev_owner,
		    ev_retry,
		    ev_type,
		    ev_data,
		    ev_extra1,
		    ev_extra2,
		    ev_extra3,
		    ev_extra4
		) VALUES (
		    %L, --ev_id
		    %L, --ev_time
		    %L, --ev_owner
		    %L, --ev_retry
		    %L, --ev_type
		    %L, --ev_data
		    %L, --ev_extra1
		    %L, --ev_extra2
		    %L, --ev_extra3
		    %L --ev_extra4
		)
	$query$;

	EXECUTE format(
		query,
		event_table.name,
		_ev_id,
		ev_time,
		ev_owner,
		ev_retry,
		ev_type,
		ev_data,
		ev_extra1,
		ev_extra2,
		ev_extra3,
		ev_extra4
	);

	RETURN _ev_id;

END;
$$ LANGUAGE plpgsql;