# frozen_string_literal: true

ts =4
NoSE::TimeDependWorkload.new do
  TimeSteps ts
  Interval 7200
  Model 'rubis_card'
  #Static true
  #FirstTs true
  #LastTs true

  def step_freq(start_ratio, end_ratio, timesteps)
    timesteps -= 1
    middle_ts = timesteps / 2
    (0..timesteps).map do |current_ts|
      current_ts <= middle_ts ? start_ratio : end_ratio
    end
  end

  step = step_freq(0.01, 0.99, ts)

  Group 'Test1', 1.0, default: step.reverse do
    Q 'SELECT count(users.firstname), count(users.email), count(users.lastname) FROM users WHERE users.id=? GROUP BY users.id -- 1'
    Q 'SELECT count(users.firstname), count(users.email), users.lastname FROM users WHERE users.lastname = ? GROUP BY users.lastname -- 0'
  end

  Group 'Test2', 1.0, default: step do
    Q 'SELECT count(items.name), count(items.initial_price), count(items.quantity) FROM items WHERE items.id=? -- 4'
    Q 'SELECT items.name, count(items.initial_price), count(items.quantity) FROM items WHERE items.name = ? GROUP BY items.name -- 3'
  end
end

