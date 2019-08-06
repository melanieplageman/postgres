# INSERT ... ON CONFLICT test verifying that speculative insertion
# failures are handled
#
# Does this by using advisory locks controlling progress of
# insertions. By waiting when building the index keys, it's possible
# to schedule concurrent INSERT ON CONFLICTs so that there will always
# be a speculative conflict.

setup
{
     CREATE OR REPLACE FUNCTION blurt_and_lock(text) RETURNS text IMMUTABLE LANGUAGE plpgsql AS $$
     BEGIN
        RAISE NOTICE 'blurt_and_lock() called for %', $1;

	-- depending on lock state, wait for lock 2 or 3
        IF pg_try_advisory_xact_lock(current_setting('spec.session')::int, 1) THEN
            RAISE NOTICE 'blocking 2';
            PERFORM pg_advisory_xact_lock(current_setting('spec.session')::int, 2);
        ELSE
            RAISE NOTICE 'blocking 3';
            PERFORM pg_advisory_xact_lock(current_setting('spec.session')::int, 3);
        END IF;
    RETURN $1;
    END;$$;

    CREATE OR REPLACE FUNCTION blurt_and_lock2(text) RETURNS text IMMUTABLE LANGUAGE plpgsql AS $$
    BEGIN
        RAISE NOTICE 'blurt_and_lock2() called for %', $1;
        PERFORM pg_advisory_xact_lock(current_setting('spec.session')::int, 4);
    RETURN $1;
    END;$$;

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
step "controller_locks" {SELECT pg_advisory_lock(sess, lock), sess, lock FROM generate_series(1, 2) a(sess), generate_series(1,3) b(lock);}
step "controller_unlock_1_1" { SELECT pg_advisory_unlock(1, 1); }
step "controller_unlock_2_1" { SELECT pg_advisory_unlock(2, 1); }
step "controller_unlock_1_2" { SELECT pg_advisory_unlock(1, 2); }
step "controller_unlock_2_2" { SELECT pg_advisory_unlock(2, 2); }
step "controller_unlock_1_3" { SELECT pg_advisory_unlock(1, 3); }
step "controller_unlock_2_3" { SELECT pg_advisory_unlock(2, 3); }
step "controller_lock_2_4" { SELECT pg_advisory_lock(2, 4); }
step "controller_unlock_2_4" { SELECT pg_advisory_unlock(2, 4); }
step "controller_show" {SELECT * FROM upserttest; }
step "controller_print_speculative_locks" { SELECT locktype,classid,objid,mode,granted FROM pg_locks WHERE locktype='speculative
token' ORDER BY granted; }

session "s1"
setup
{
  SET default_transaction_isolation = 'read committed';
  SET spec.session = 1;
}
step "s1_begin"  { BEGIN; }
step "s1_create_non_unique_index" { CREATE INDEX upserttest_key_idx ON upserttest((blurt_and_lock2(key))); }
step "s1_confirm_index_order" { SELECT 'upserttest_key_uniq_idx'::regclass::int8 < 'upserttest_key_idx'::regclass::int8; }
step "s1_upsert" { INSERT INTO upserttest(key, data) VALUES('k1', 'inserted s1') ON CONFLICT (blurt_and_lock(key)) DO UPDATE SET data = upserttest.data || ' with conflict update s1'; }
step "s1_commit"  { COMMIT; }

session "s2"
setup
{
  SET default_transaction_isolation = 'read committed';
  SET spec.session = 2;
}
step "s2_begin"  { BEGIN; }
step "s2_upsert" { INSERT INTO upserttest(key, data) VALUES('k1', 'inserted s2') ON CONFLICT (blurt_and_lock(key)) DO UPDATE SET data = upserttest.data || ' with conflict update s2'; }
step "s2_commit"  { COMMIT; }

# Test that speculative locks are correctly acquired and released, s2
# inserts, s1 updates.
permutation
   # acquire a number of locks, to control execution flow - the
   # blurt_and_lock function acquires advisory locks that allow us to
   # continue after a) the optimistic conflict probe b) after the
   # insertion of the speculative tuple.
   "controller_locks"
   "controller_show"
   "s1_upsert" "s2_upsert"
   "controller_show"
   # Switch both sessions to wait on the other lock next time (the speculative insertion)
   "controller_unlock_1_1" "controller_unlock_2_1"
   # Allow both sessions to continue
   "controller_unlock_1_3" "controller_unlock_2_3"
   "controller_show"
   # Allow the second session to finish insertion
   "controller_unlock_2_2"
   # This should now show a successful insertion
   "controller_show"
   # Allow the first session to finish insertion
   "controller_unlock_1_2"
   # This should now show a successful UPSERT
   "controller_show"

# Test that speculative locks are correctly acquired and released, s1
# inserts, s2 updates.
permutation
   # acquire a number of locks, to control execution flow - the
   # blurt_and_lock function acquires advisory locks that allow us to
   # continue after a) the optimistic conflict probe b) after the
   # insertion of the speculative tuple.
   "controller_locks"
   "controller_show"
   "s1_upsert" "s2_upsert"
   "controller_show"
   # Switch both sessions to wait on the other lock next time (the speculative insertion)
   "controller_unlock_1_1" "controller_unlock_2_1"
   # Allow both sessions to continue
   "controller_unlock_1_3" "controller_unlock_2_3"
   "controller_show"
   # Allow the first session to finish insertion
   "controller_unlock_1_2"
   # This should now show a successful insertion
   "controller_show"
   # Allow the second session to finish insertion
   "controller_unlock_2_2"
   # This should now show a successful UPSERT
   "controller_show"

# Test that speculative locks are correctly acquired and released, s2
# inserts, s1 updates.  With the added complication that transactions
# don't immediately commit.
permutation
   # acquire a number of locks, to control execution flow - the
   # blurt_and_lock function acquires advisory locks that allow us to
   # continue after a) the optimistic conflict probe b) after the
   # insertion of the speculative tuple.
   "controller_locks"
   "controller_show"
   "s1_begin" "s2_begin"
   "s1_upsert" "s2_upsert"
   "controller_show"
   # Switch both sessions to wait on the other lock next time (the speculative insertion)
   "controller_unlock_1_1" "controller_unlock_2_1"
   # Allow both sessions to continue
   "controller_unlock_1_3" "controller_unlock_2_3"
   "controller_show"
   # Allow the first session to finish insertion
   "controller_unlock_1_2"
   # But the change isn't visible yet, nor should the second session continue
   "controller_show"
   # Allow the second session to finish insertion, but it's blocked
   "controller_unlock_2_2"
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
   # blurt_and_lock2 acquires an advisory lock which allows us to pause
   # execution c) before completing the speculative insertion

   # create the second index here to avoid affecting the other
   # permutations.
   "s1_create_non_unique_index"
   # confirm that the insertion into the unique index will happen first
   "s1_confirm_index_order"
   "controller_locks"
   "controller_show"
   # Both sessions wait on advisory locks
   "s1_upsert" "s2_upsert"
   "controller_show"
   # Switch both sessions to wait on the other lock next time (the speculative insertion)
   "controller_unlock_1_1" "controller_unlock_2_1"
   # Allow both sessions to do the optimistic conflict probe and do the
   # speculative insertion into the table
   # They will then be waiting on another advisory lock when they attempt to
   # update the index
   "controller_unlock_1_3" "controller_unlock_2_3"
   "controller_show"
   # take lock to block second session after inserting in unique index but
   # before completing the speculative insert
   "controller_lock_2_4"
   # Allow the second session to move forward
   "controller_unlock_2_2"
   # This should still not show a successful insertion
   "controller_show"
   # Allow the first session to continue, it should perform speculative wait
   "controller_unlock_1_2"
   # Should report s1 is waiting on speculative lock
   "controller_print_speculative_locks"
   # Allow s2 to insert into the non-unique index and complete
   # s1 will no longer wait and will proceed to update
   "controller_unlock_2_4"
   # This should now show a successful UPSERT
   "controller_show"
