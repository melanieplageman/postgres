# INSERT ... ON CONFLICT test verifying that speculative insertion
# failures are handled
#
# Does this by using advisory locks controlling progress of
# insertions. By waiting when building the index keys, it's possible
# to schedule concurrent INSERT ON CONFLICTs so that there will always
# be a speculative conflict.

setup
{

	CREATE OR REPLACE FUNCTION block_on_check_index_constraints() returns
	bool VOLATILE LANGUAGE plpgsql AS $$
	BEGIN
	    IF pg_try_advisory_xact_lock(current_setting('spec.session')::int, 1) THEN
		   RETURN FALSE;
		ELSE
		   RETURN TRUE;
		END IF;
	END; $$;

     CREATE OR REPLACE FUNCTION blurt_and_lock(text) RETURNS text IMMUTABLE LANGUAGE plpgsql AS $$
	 DECLARE
	     WAIT_ON_INDEX_INSERT_LOCK INT := 2;
	     CHECK_INDEX_CONSTRAINTS_LOCK INT := 3;
     BEGIN
        RAISE NOTICE 'blurt_and_lock() called for %', $1;

	-- depending on lock state, wait for lock 2 or 3
        IF block_on_check_index_constraints() THEN
            RAISE NOTICE 'blocking CHECK_INDEX_CONSTRAINTS_LOCK';
            PERFORM pg_advisory_xact_lock(current_setting('spec.session')::int, CHECK_INDEX_CONSTRAINTS_LOCK);
		-- Block on insert instead
        ELSE
            RAISE NOTICE 'blocking WAIT_ON_INDEX_INSERT_LOCK';
            PERFORM pg_advisory_xact_lock(current_setting('spec.session')::int,
			WAIT_ON_INDEX_INSERT_LOCK);
        END IF;
    RETURN $1;
    END;$$;


    CREATE OR REPLACE FUNCTION blurt_and_lock_before_complete(text) RETURNS text IMMUTABLE LANGUAGE plpgsql AS $$
	 DECLARE
	     WAIT_ON_COMPLETE_SPECULATIVE_INSERT_LOCK INT := 4;
    BEGIN
        RAISE NOTICE 'blurt_and_lock_before_complete() called for %', $1;
        PERFORM pg_advisory_xact_lock(current_setting('spec.session')::int, 4);
    RETURN $1;
    END;$$;

    CREATE OR REPLACE FUNCTION ctoast_large_val() RETURNS TEXT LANGUAGE SQL AS 'select array_agg(md5(g::text))::text from generate_series(1, 256) g';

    CREATE TABLE upserttest(key text, data text);

    CREATE UNIQUE INDEX upserttest_key_uniq_idx ON upserttest((blurt_and_lock(key)));
}

teardown
{
    DROP TABLE upserttest;
}

session "controller"
setup
{
  SET default_transaction_isolation = 'read committed';
}
step "controller_init_locks" { SELECT pg_advisory_lock(1, 2); SELECT pg_advisory_lock(1, 3); SELECT pg_advisory_lock(2, 2); SELECT pg_advisory_lock(2, 3); }
step "controller_toggle_on_wait_condition" { SELECT pg_advisory_lock(1, 1); SELECT pg_advisory_lock(2, 1); }
step "controller_toggle_off_wait_condition" { SELECT pg_advisory_unlock(1, 1); SELECT pg_advisory_unlock(2, 1); }
step "controller_unlock_index_insert_lock_s1" { SELECT pg_advisory_unlock(1, 2); }
step "controller_unlock_index_insert_lock_s2" { SELECT pg_advisory_unlock(2, 2); }
step "controller_unlock_index_constraint_check_lock_s1" { SELECT pg_advisory_unlock(1, 3); }
step "controller_unlock_index_constraint_check_lock_s2" { SELECT pg_advisory_unlock(2, 3); }
step "controller_lock_2_4" { SELECT pg_advisory_lock(2, 4); }
step "controller_unlock_2_4" { SELECT pg_advisory_unlock(2, 4); }
step "controller_show" {SELECT * FROM upserttest; }
step "controller_show_count" {SELECT COUNT(*) FROM upserttest; }
step "controller_print_speculative_locks" { SELECT locktype,classid,objid,mode,granted FROM pg_locks WHERE locktype='speculative
token' ORDER BY granted; }

session "s1"
setup
{
  SET default_transaction_isolation = 'read committed';
  SET spec.session = 1;
}
step "s1_begin"  { BEGIN; }
step "s1_create_non_unique_index" { CREATE INDEX upserttest_key_idx ON upserttest((blurt_and_lock_before_complete(key))); }
step "s1_confirm_index_order" { SELECT 'upserttest_key_uniq_idx'::regclass::int8 < 'upserttest_key_idx'::regclass::int8; }
step "s1_upsert" { INSERT INTO upserttest(key, data) VALUES('k1', 'inserted s1') ON CONFLICT (blurt_and_lock(key)) DO UPDATE SET data = upserttest.data || ' with conflict update s1'; }
step "s1_insert_toast" { INSERT INTO upserttest VALUES('k2', ctoast_large_val()) ON CONFLICT DO NOTHING; }
step "s1_commit"  { COMMIT; }

session "s2"
setup
{
  SET default_transaction_isolation = 'read committed';
  SET spec.session = 2;
}
step "s2_begin"  { BEGIN; }
step "s2_upsert" { INSERT INTO upserttest(key, data) VALUES('k1', 'inserted s2') ON CONFLICT (blurt_and_lock(key)) DO UPDATE SET data = upserttest.data || ' with conflict update s2'; }
step "s2_insert_toast" { INSERT INTO upserttest VALUES('k2', ctoast_large_val()) ON CONFLICT DO NOTHING; }
step "s2_commit"  { COMMIT; }

# Test that speculative locks are correctly acquired and released, s2
# inserts, s1 updates.
permutation
   # acquire a number of locks, to control execution flow - the
   # blurt_and_lock function acquires advisory locks that allow us to
   # continue after a) the optimistic conflict probe b) after the
   # insertion of the speculative tuple.
   "controller_init_locks"
   "controller_toggle_on_wait_condition"
   "controller_show"
   "s1_upsert" "s2_upsert"
   "controller_show"
   # Switch both sessions to wait on the other lock next time (the speculative insertion)
   "controller_toggle_off_wait_condition"
   # Allow both sessions to continue
   "controller_unlock_index_constraint_check_lock_s1" "controller_unlock_index_constraint_check_lock_s2"
   "controller_show"
   # Allow the second session to finish insertion
   "controller_unlock_index_insert_lock_s2"
   # This should now show a successful insertion
   "controller_show"
   # Allow the first session to finish insertion
   "controller_unlock_index_insert_lock_s1"
   # This should now show a successful UPSERT
   "controller_show"

# Test that speculative locks are correctly acquired and released, s1
# inserts, s2 updates.
permutation
   # acquire a number of locks, to control execution flow - the
   # blurt_and_lock function acquires advisory locks that allow us to
   # continue after a) the optimistic conflict probe b) after the
   # insertion of the speculative tuple.
   "controller_init_locks"
   "controller_toggle_on_wait_condition"
   "controller_show"
   "s1_upsert" "s2_upsert"
   "controller_show"
   # Switch both sessions to wait on the other lock next time (the speculative insertion)
   "controller_toggle_off_wait_condition"
   # Allow both sessions to continue
   "controller_unlock_index_constraint_check_lock_s1" "controller_unlock_index_constraint_check_lock_s2"
   "controller_show"
   # Allow the first session to finish insertion
   "controller_unlock_index_insert_lock_s1"
   # This should now show a successful insertion
   "controller_show"
   # Allow the second session to finish insertion
   "controller_unlock_index_insert_lock_s2"
   # This should now show a successful UPSERT
   "controller_show"

# Test that speculatively inserted toast rows do not cause conflicts.
# s1 inserts successfully, s2 does not.
permutation
   # acquire a number of locks, to control execution flow - the
   # blurt_and_lock function acquires advisory locks that allow us to
   # continue after a) the optimistic conflict probe b) after the
   # insertion of the speculative tuple.
   "controller_init_locks"
   "controller_toggle_on_wait_condition"
   "controller_show"
   "s1_insert_toast" "s2_insert_toast"
   "controller_show"
   # Switch both sessions to wait on the other lock next time (the speculative insertion)
   "controller_toggle_off_wait_condition"
   # Allow both sessions to continue
   "controller_unlock_index_constraint_check_lock_s1" "controller_unlock_index_constraint_check_lock_s2"
   "controller_show"
   # Allow the first session to finish insertion
   "controller_unlock_index_insert_lock_s1"
   # This should now show that 1 additional tuple was inserted successfully
   "controller_show_count"
   # Allow the second session to finish insertion and kill the speculatively inserted tuple
   "controller_unlock_index_insert_lock_s2"
   # This should show the same number of tuples as before s2 inserted
   "controller_show_count"

# Test that speculative locks are correctly acquired and released, s2
# inserts, s1 updates.  With the added complication that transactions
# don't immediately commit.
permutation
   # acquire a number of locks, to control execution flow - the
   # blurt_and_lock function acquires advisory locks that allow us to
   # continue after a) the optimistic conflict probe b) after the
   # insertion of the speculative tuple.
   "controller_init_locks"
   "controller_toggle_on_wait_condition"
   "controller_show"
   "s1_begin" "s2_begin"
   "s1_upsert" "s2_upsert"
   "controller_show"
   # Switch both sessions to wait on the other lock next time (the speculative insertion)
   "controller_toggle_off_wait_condition"
   # Allow both sessions to continue
   "controller_unlock_index_constraint_check_lock_s1" "controller_unlock_index_constraint_check_lock_s2"
   "controller_show"
   # Allow the first session to finish insertion
   "controller_unlock_index_insert_lock_s1"
   # But the change isn't visible yet, nor should the second session continue
   "controller_show"
   # Allow the second session to finish insertion, but it's blocked
   "controller_unlock_index_insert_lock_s2"
   "controller_show"
   # But committing should unblock
   "s1_commit"
   "controller_show"
   "s2_commit"
   "controller_show"

# Test that speculative wait is performed if a session sees a speculatively
# inserted tuple. A speculatively inserted tuple is one which has been inserted
# both into the table and the unique index but has yet to *complete* the
# speculative insertion
permutation
   # acquire a number of advisory locks to control execution flow - the
   # blurt_and_lock function acquires advisory locks that allow us to
   # continue after a) the optimistic conflict probe and b) after the
   # insertion of the speculative tuple.
   # blurt_and_lock_before_complete acquires an advisory lock which allows us to pause
   # execution c) before completing the speculative insertion

   # create the second index here to avoid affecting the other
   # permutations.
   "s1_create_non_unique_index"
   # confirm that the insertion into the unique index will happen first
   "s1_confirm_index_order"
   "controller_init_locks"
   "controller_toggle_on_wait_condition"
   "controller_show"
   # Both sessions wait on advisory locks
   "s1_upsert" "s2_upsert"
   "controller_show"
   # Switch both sessions to wait on the other lock next time (the speculative insertion)
   "controller_toggle_off_wait_condition"
   # Allow both sessions to do the optimistic conflict probe and do the
   # speculative insertion into the table
   # They will then be waiting on another advisory lock when they attempt to
   # update the index
   "controller_unlock_index_constraint_check_lock_s1" "controller_unlock_index_constraint_check_lock_s2"
   "controller_show"
   # take lock to block second session after inserting in unique index but
   # before completing the speculative insert
   "controller_lock_2_4"
   # Allow the second session to move forward
   "controller_unlock_index_insert_lock_s2"
   # This should still not show a successful insertion
   "controller_show"
   # Allow the first session to continue, it should perform speculative wait
   "controller_unlock_index_insert_lock_s1"
   # Should report s1 is waiting on speculative lock
   "controller_print_speculative_locks"
   # Allow s2 to insert into the non-unique index and complete
   # s1 will no longer wait and will proceed to update
   "controller_unlock_2_4"
   # This should now show a successful UPSERT
   "controller_show"
