# TODO

 * Evaluate setting yield_zero to False for workload generator nodes
 * ? Analyse file unique bytes, file total bytes, accesses as currently created

 * further stop recording predicate (TODO unique bytes read, TODO total bytes read, no of accesses, time)
   -> requires embedding / referencing of a stats collector
   proper stats counting would require transactioning: testing the effect of
   the access but only perform the access when a condition passes
 * further warm-up predicate (TODO unique bytes read, TODO total bytes read, no of accesses, time)
   -> requires embedding / referencing of a stats collector
   proper stats counting would require transactioning: testing the effect of
   the access but only perform the access when a condition passes

 * Improved pattern for the _iterator wrapper_ approach

   ATM the wrapping class receives the iterator upon construction. The
   iterator is stored as a class member. Hence, iter(wrapper_inst) returns a
   ready iterator. Storing the underlying iterator in the class makes only
   some sense. It would be better to accept the underlying iterator as an
   argument when constructing the internal iterator.

   The wrapping iterator itself is often created through a generator function.
   This function already has internal (locals) which could include the
   underlying iterator. Only in some cases is further state stored in the
   class, generally for external access of output state (e.g. StatsCollector).

   A function taking the iterator and returning a state accessor which also
   represents the wrapping (enhanced) iterator would be ideal. That
   essentially is the current approach, but the current approach accepts
   configuration along with the source iterator. This could be transformed
   into a single argument function call using currying, this would result in a
   specification container which would create the iterator wrapper with its
   specification.

 * algorithms

   * EVA

   * EVA-Bit

   * Fix existing ones: Don't evict file about to be accessed

     Might be simpler with a different cache processor interface which
     simplifies processing to a single method call which can interact with the
     cache system, i.e. cause evictions. The pattern would be: if currently in
     the cache mark the file to be accessed as the least likely element to be
     evicted. While insufficient space evict some element. Update element
     describing the file about to be accessed with new information.

   * PRP (simple form), sensible?

   * PRP-Bit OBMA, sensible?

   * PRP / PRP-Bit with custom classification

   * Investigate ARC-Bit

   * LRFU (sampling or full pq)

   * gLRU, gRAND

   * Workload-derived caching policy

 * cache bypassing?

   Implementing this semantic would allow arbitrary sized files as well as
   cache admission policies, thereby enabling simulation of optional caching.

 * logging
   * log the seed (and whether passed or generated)
   * ? stages/progress
   * failures of runnables
   * warnings, for example
     * task overlap in scheduler
     * evictions of the accessed file in state processor
     * drawing a > 99 % value from a distribution? (probably an indication that other distributions are required)

* node / task (submitter) / job stats

  * node: (count of submitters)
  * submitter: (known_time, start, job_start, end) {duration}
  * job: (submit_time, start, end) {duration, queued_time}

  * write "from" information to submitter (could be node or str, must be optional)
  * map jobs to submitters, must happen close to merger, e.g. in wrap jobs submission in meta-data enhancing iterable
  * analyse job stream after merger
  * store results in-mem (must do some things for proper calculations) or have a backend for writing to file?

 * Remove Take While interface from Reader

   * replace filter_accesses_stop_early with a wrapper around StopEarlyPredicate

 * Improve filtering/including/dropping the cache processor index from access assignments through refactoring

 * Refactor state module: De-inner types, declare mix-in, include common Item type (used for remove), insufficient cache capacity exception, rename process_access of State type into something more like "process_access_notification" (State is notified about the access after all processing has occurred)

 * accessseq

   * Look at the number of accesses to figure out the width of the array required, count parts and max parts index, max time stamp
     * Do this by first performing a full read of the accesses sequence (this also calculates length in Reader)
   * FullReuseIndex.build(): Calculate next_use_ind by "reversing" prev_use_ind
   * generalised method `_reuses_following`

 * CLI tool for calculating precise bytes access frequencies

   Use the FullReuseIndex to count the exact number of reuses for each known byte, by following reuses of each file and then each part

   For the workload models used this is not important, as each part is always accessed in full

 * dstructure: Fast weighted random selection via tree

   Problem with binary search on cumsum list: Slow to update (O(n)).

   Storing weights in an explicit tree, where inner nodes contain the sum of weights of its descendent leaves.

   Updates take O(log(n)) and weighted selection also takes O(log(n))

 * Improve inconsistent stats column names, for example unclear `total_` prefix referring both to a counter over the entire access sequence so far and the overall

 * cache-info-stats command for calculating cache performance statistics on a cache_info output file

   For example hit and missed bytes with different warm-up and process periods

 * Alternative distributions for schedules: Capped at p-percentile and (100-p)-percentile.

   Any more extreme values lead to re-drawing from the underlying distribution.

 * Many-agent, unsynchronised cache processors

   * Remove `ensure` argument in cache processors `process_access()` method

     In a distributed cache processors environment, the cache processor may not track the file, thus, the processor must ensure the file is tracked.
     So there is little point in having an `ensure` argument.

   * When a cache processor fails to evict with its own state, fallback to reading all files on the cache volume and evicting random ones until enough space has been freed.
     Otherwise, the access would fail, the simulator throws an exception in this case.

     May rely on the insufficient cache capacity exception mentioned above.
     This way, the StateProcessor can implement this behaviour as a fallback, without having to amend the algorithms.

 * There is a lot of somewhat duplicated code regarding calculation and filtering of PartSpecs

   Introducing some utilities types should aid in simplifying the code in:

   * accessseq
   * cache.stats
   * storage
