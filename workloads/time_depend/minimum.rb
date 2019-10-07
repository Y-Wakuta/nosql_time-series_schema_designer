# frozen_string_literal: true

NoSE::TimeDependWorkload.new do
  Model 'rubis'

  # Define queries and their relative weights, weights taken from below
  # http://rubis.ow2.org/results/SB-BMP/Bidding/JBoss-SB-BMP-Bi-1500/perf.html#run_stat
  # http://rubis.ow2.org/results/SB-BMP/Browsing/JBoss-SB-BMP-Br-1500/perf.html#run_stat
  DefaultMix :default
  TimeSteps 3
  Interval 3600

  Group 'UsersInfo', 1.0, default: [0.001, 0.5, 9] do
    Q 'SELECT users.* FROM users WHERE users.id = ? -- 8'
    Q 'SELECT users.* FROM users WHERE users.rating = ? -- 8'
    Q 'UPDATE users SET rating=? WHERE users.id=? -- 27'
  end

  Group 'ItemsInfo', 1.0, default: [9, 0.5, 0.001] do
    Q 'SELECT items.* FROM items WHERE items.id=? -- 13'
    Q 'SELECT items.* FROM items WHERE items.quantity=? -- 13 LIMIT 1'
    Q 'UPDATE items SET nb_of_bids=? WHERE items.id=? -- 22'
  end
end
