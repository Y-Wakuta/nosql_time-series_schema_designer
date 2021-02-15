# frozen_string_literal: true

ts = 10
NoSE::TimeDependWorkload.new do
  TimeSteps ts
  Interval 4000
  Model 'rubis'
  #Static true

  def step_freq(start_ratio, end_ratio, timesteps)
    timesteps -= 1
    middle_ts = timesteps / 2
    (0..timesteps).map do |current_ts|
      current_ts <= middle_ts ? start_ratio : end_ratio
    end
  end

  step = step_freq(0.1, 0.9, ts)

  Group 'Test1', 1.0, default: step.reverse do
    Q 'SELECT users.* FROM users WHERE users.id=? -- 1'
    Q 'SELECT users.* FROM users WHERE users.firstname = ? -- 0'
    Q 'INSERT INTO users SET id = ?, firstname=?, lastname = ?, nickname=?, password=?,email=?,rating=?,balance=?,creation_date=? -- 2'
  end

  Group 'Test2', 1.0, default: step do
    Q 'SELECT items.* FROM items WHERE items.id=? -- 4'
    Q 'SELECT items.* FROM items WHERE items.name = ? -- 3'
    Q 'INSERT INTO items SET id = ?, name=?, description = ?, initial_price=?,quantity=?, reserve_price=?, buy_now=?, nb_of_bids=?, max_bid=?,start_date=?,end_date=? -- 5.size'
  end
end

