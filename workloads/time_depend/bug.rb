# frozen_string_literal: true

NoSE::TimeDependWorkload.new do
  Model 'rubis'

  DefaultMix :browsing
  TimeSteps 3

  Group 'UsersInfo', 1.0, browsing: 4.41,
        write_heavy: 2.48 do
    Q 'UPDATE items SET nb_of_bids=? WHERE items.id=? -- 22', [0.1, 0.2, 300]
    Q 'SELECT items.* FROM items.bids.user WHERE user.id=?', [300, 0.2, 0.1]
   # Q 'UPDATE items SET nb_of_bids=? WHERE items.id=? -- 22', [0.1, 0.2]
   # Q 'SELECT items.* FROM items.bids.user WHERE user.id=?', [0.2, 0.1]
  end
end
