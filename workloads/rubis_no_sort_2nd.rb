# frozen_string_literal: true

NoSE::Workload.new do
  Model 'rubis'

  DefaultMix :browsing

  Group 'ViewBidHistory', browsing: 2.38 do
    Q 'SELECT users.* FROM users WHERE users.rating = ? -- 8_secondary'
 #   Q 'SELECT items.* FROM items.bids WHERE items.quantity = ? -- 6'
  end

  Group 'AboutMe', browsing: 1.71 do
    Q 'SELECT items.* FROM items.bids.user WHERE user.rating =? -- 34_secondary'
   # Q 'SELECT items.* FROM items WHERE items.quantity = ? -- 6'
  end

end
