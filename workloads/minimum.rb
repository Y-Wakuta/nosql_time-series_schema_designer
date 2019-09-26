# frozen_string_literal: true

NoSE::Workload.new do
  Model 'rubis'

  # Define queries and their relative weights, weights taken from below
  # http://rubis.ow2.org/results/SB-BMP/Bidding/JBoss-SB-BMP-Bi-1500/perf.html#run_stat
  # http://rubis.ow2.org/results/SB-BMP/Browsing/JBoss-SB-BMP-Br-1500/perf.html#run_stat
  DefaultMix :browsing

  Group 'UsersInfo', browsing: 4.41,
                        bidding: 2.48,
                        write_medium: 2.48,
                        write_heavy: 2.48 do
    Q 'SELECT users.* FROM users WHERE users.id = ? -- 8'
    Q 'SELECT users.* FROM users WHERE users.rating=? -- 12'
  end

  Group 'ItemsInfo', browsing: 8.82,
        bidding: 5.96,
        write_medium: 4.96,
        write_heavy: 4.96 do
    Q 'SELECT items.* FROM items WHERE items.id=? -- 13'
    Q 'SELECT items.* FROM items WHERE items.quantity=? -- 13'
  end
end
