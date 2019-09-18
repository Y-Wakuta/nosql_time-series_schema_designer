# frozen_string_literal: true

NoSE::Workload.new do
  Model 'rubis'

  # Define queries and their relative weights, weights taken from below
  # http://rubis.ow2.org/results/SB-BMP/Bidding/JBoss-SB-BMP-Bi-1500/perf.html#run_stat
  # http://rubis.ow2.org/results/SB-BMP/Browsing/JBoss-SB-BMP-Br-1500/perf.html#run_stat
  DefaultMix :browsing

  Group 'BrowseCategories', browsing: 4.44,
        bidding: 7.65,
        write_medium: 7.65,
        write_heavy: 7.65 do
    #Q 'SELECT users.lastname FROM users WHERE users.rating = ? -- 8'
    #Q 'SELECT users.firstname, users.nickname FROM users WHERE users.rating = ? -- 9'
    #Q 'SELECT bids.qty, bids.date FROM bids.item WHERE item.id=? ORDER BY bids.bid LIMIT 2 -- 19'

    Q 'SELECT items.id, items.nb_of_bids, items.end_date FROM items.category WHERE ' \
      'category.id = ? AND items.end_date >= ? LIMIT 25 -- 7'
  end
end
