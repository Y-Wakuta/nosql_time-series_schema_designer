# frozen_string_literal: true

NoSE::TimeDependWorkload.new do
  Model 'rubis'

  # Define queries and their relative weights, weights taken from below
  # http://rubis.ow2.org/results/SB-BMP/Bidding/JBoss-SB-BMP-Bi-1500/perf.html#run_stat
  # http://rubis.ow2.org/results/SB-BMP/Browsing/JBoss-SB-BMP-Br-1500/perf.html#run_stat
  DefaultMix :bidding

  def linear_freq(start_ratio, end_ratio, timesteps)
    timesteps -= 1
    (0..timesteps).map do |current_ts|
      (((end_ratio - start_ratio) / timesteps) * current_ts + start_ratio).round(5)
    end
  end

  linear = linear_freq(0.001, 0.999, 10)

  frequencies = linear


  #increase = [0.1, 3, 8, 20, 30, 5, 1, 0.01, 10, 100, 50, 10, 1, 0.1, 20]
  #increase = [0.1, 3, 8, 20]
  #increase = (0..9).map{|i| 2 **i}
  #decrease = increase.reverse

  timestep = linear.size
  TimeSteps timestep
  Interval 200

  #increase_func = Proc.new() {|x_coef, t, y| (0..t).map{|t_| x_coef * t_ * t_ * t_ + y}}
  #decrease_func = Proc.new() {|x_coef, t, y| (0..t).map{|t_| [-x_coef * t_ * t_ * t_ + y, 0.001].max}}
  #increase = increase_func.call(5, timestep, 0.0001)
  #decrease = decrease_func.call(5, timestep, 100.001)

  Group 'BrowseCategories', browsing: linear.map{|l| l * 4.44},
        bidding: linear.map{|l| l * 7.65},
        write_medium: linear.map{|l| l * 7.65},
        write_heavy: linear.map{|l| l * 7.65} do
    Q 'SELECT users.nickname, users.password FROM users WHERE users.id = ? -- 1'
    # XXX Must have at least one equality predicate
    Q 'SELECT categories.id, categories.name FROM categories WHERE ' \
      'categories.dummy = 1 -- 2'
  end

    Group 'ViewBidHistory', browsing: linear.map{|l| l * 2.38},
                          bidding: linear.map{|l| l * 1.54},
                          write_medium: linear.map{|l| l * 1.54},
                          write_heavy: linear.map{|l| l * 1.54} do
    Q 'SELECT items.name FROM items WHERE items.id = ? -- 3'
    Q 'SELECT users.id, users.nickname, bids.id, item.id, bids.qty, ' \
      'bids.bid, bids.date FROM users.bids.item WHERE item.id = ? ' \
      'ORDER BY bids.date -- 4'
  end

  Group 'ViewItem', browsing: linear.map{|l| l * 22.95},
                    bidding: linear.map{|l| l * 14.17},
                    write_medium: linear.map{|l| l * 14.17},
                    write_heavy: linear.map{|l| l * 14.17} do
    Q 'SELECT items.* FROM items WHERE items.id = ? -- 5'
    Q 'SELECT bids.* FROM items.bids WHERE items.id = ? -- 6'
  end

  Group 'SearchItemsByCategory', browsing: linear.map{|l| l *  27.77},
                                 bidding: linear.map{|l| l * 15.94},
                                 write_medium: linear.map{|l| l * 15.94},
                                 write_heavy: linear.map{|l| l * 15.94} do
    Q 'SELECT items.id, items.name, items.initial_price, items.max_bid, ' \
      'items.nb_of_bids, items.end_date FROM items.category WHERE ' \
      'category.id = ? AND items.end_date >= ? LIMIT 25 -- 7'
  end

  Group 'ViewUserInfo', browsing: linear.map{|l| l * 4.41},
                        bidding: linear.map{|l| l * 2.48},
                        write_medium: linear.map{|l| l * 2.48},
                        write_heavy: linear.map{|l| l * 2.48} do
    # XXX Not including region name below
    Q 'SELECT users.* FROM users WHERE users.id = ? -- 8'
    Q 'SELECT comments.id, comments.rating, comments.date, comments.comment ' \
      'FROM comments.to_user WHERE to_user.id = ? -- 9'
  end

  Group 'RegisterItem', bidding: linear.reverse.map{|l| l * 0.53},
                        write_medium: linear.reverse.map{|l| l * 0.53 * 10},
                        write_heavy: linear.reverse.map{|l| l * 0.53 * 100} do
    Q 'INSERT INTO items SET id=?, name=?, description=?, initial_price=?, ' \
      'quantity=?, reserve_price=?, buy_now=?, nb_of_bids=0, max_bid=0, ' \
      'start_date=?, end_date=? AND CONNECT TO category(?), seller(?) -- 10'
  end

  Group 'RegisterUser', bidding: linear.reverse.map{|l| l * 1.07},
                        write_medium: linear.reverse.map{|l| l * 1.07 * 10},
                        write_heavy: linear.reverse.map{|l| l * 1.07 * 100} do
    Q 'INSERT INTO users SET id=?, firstname=?, lastname=?, nickname=?, ' \
      'password=?, email=?, rating=0, balance=0, creation_date=? ' \
      'AND CONNECT TO region(?) -- 11'
  end

  Group 'BuyNow', bidding: linear.map{|l| l * 1.16},
                  write_medium: linear.map{|l| l * 1.16},
                  write_heavy: linear.map{|l| l * 1.16} do
    Q 'SELECT users.nickname FROM users WHERE users.id=? -- 12'
    Q 'SELECT items.* FROM items WHERE items.id=? -- 13'
  end

  Group 'StoreBuyNow', bidding: linear.reverse.map{|l| l * 1.10},
                       write_medium: linear.reverse.map{|l| l * 1.10 * 10},
                       write_heavy: linear.reverse.map{|l| l * 1.10 * 100} do
    Q 'SELECT items.quantity, items.nb_of_bids, items.end_date FROM items ' \
      'WHERE items.id=? -- 14'
    Q 'UPDATE items SET quantity=?, nb_of_bids=?, end_date=? WHERE items.id=? -- 15'
    Q 'INSERT INTO buynow SET id=?, qty=?, date=? ' \
      'AND CONNECT TO item(?), buyer(?) -- 16'
  end

  Group 'PutBid', bidding: linear.map{|l| l * 5.40},
                  write_medium: linear.map{|l| l * 5.40},
                  write_heavy: linear.map{|l| l * 5.40} do
    Q 'SELECT users.nickname, users.password FROM users WHERE users.id=? -- 17'
    Q 'SELECT items.* FROM items WHERE items.id=? -- 18'
    Q 'SELECT bids.qty, bids.date FROM bids.item WHERE item.id=? ' \
      'ORDER BY bids.bid LIMIT 2 -- 19'
  end

  Group 'StoreBid', bidding: linear.reverse.map{|l| l * 3.74},
                    write_medium: linear.reverse.map{|l| l * 3.74 * 10},
                    write_heavy: linear.reverse.map{|l| l * 3.74 * 100} do
    Q 'INSERT INTO bids SET id=?, qty=?, bid=?, date=? ' \
      'AND CONNECT TO item(?), user(?) -- 20'
    Q 'SELECT items.nb_of_bids, items.max_bid FROM items WHERE items.id=? -- 21'
    Q 'UPDATE items SET nb_of_bids=?, max_bid=? WHERE items.id=? -- 22'
  end

  Group 'PutComment', bidding: linear.map{|l| l * 0.46},
                      write_medium: linear.map{|l| l * 0.46},
                      write_heavy: linear.map{|l| l * 0.46} do
    Q 'SELECT users.nickname, users.password FROM users WHERE users.id=? -- 23'
    Q 'SELECT items.* FROM items WHERE items.id=? -- 24'
    Q 'SELECT users.* FROM users WHERE users.id=? -- 25'
  end

  Group 'StoreComment', bidding: linear.reverse.map{|l| l * 0.45},
                        write_medium: linear.reverse.map{|l| l * 0.45 * 10},
                        write_heavy: linear.reverse.map{|l| l * 0.45 * 100} do
    Q 'SELECT users.rating FROM users WHERE users.id=? -- 26'
    Q 'UPDATE users SET rating=? WHERE users.id=? -- 27'
    Q 'INSERT INTO comments SET id=?, rating=?, date=?, comment=? ' \
      'AND CONNECT TO to_user(?), from_user(?), item(?) -- 28'
  end

  Group 'AboutMe', bidding: linear.map{|l| l * 1.71},
                   write_medium: linear.map{|l| l * 1.71},
                   write_heavy: linear.map{|l| l * 1.71} do
    Q 'SELECT users.* FROM users WHERE users.id=? -- 29'
    Q 'SELECT comments_received.* FROM users.comments_received ' \
      'WHERE users.id = ? -- 30'
    Q 'SELECT from_user.nickname FROM comments.from_user WHERE comments.id = ? -- 31'
    Q 'SELECT bought_now.*, items.* FROM items.bought_now.buyer ' \
      'WHERE buyer.id = ? AND bought_now.date>=? -- 32'
    Q 'SELECT items.* FROM items.seller WHERE seller.id=? AND ' \
      'items.end_date >=? -- 33'
    Q 'SELECT items.* FROM items.bids.user WHERE user.id=? AND ' \
      'items.end_date>=? -- 34'
  end

  Group 'SearchItemsByRegion', browsing: linear.map{|l| l * 8.26},
                               bidding: linear.map{|l| l * 6.34},
                               write_medium: linear.map{|l| l * 6.34},
                               write_heavy: linear.map{|l| l * 6.34} do
    Q 'SELECT items.id, items.name, items.initial_price, items.max_bid, ' \
      'items.nb_of_bids, items.end_date FROM ' \
      'items.seller WHERE seller.region.id = ? AND items.category.id = ? ' \
      'AND items.end_date >= ? LIMIT 25 -- 35'
  end

  Group 'BrowseRegions', browsing: linear.map{|l| l * 3.21},
                         bidding: linear.map{|l| l * 5.39},
                         write_medium: linear.map{|l| l * 5.39},
                         write_heavy: linear.map{|l| l * 5.39} do
    # XXX Must have at least one equality predicate
    Q 'SELECT regions.id, regions.name FROM regions ' \
      'WHERE regions.dummy = 1 -- 36'
  end
end
