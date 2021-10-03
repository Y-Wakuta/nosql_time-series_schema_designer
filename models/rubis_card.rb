# frozen_string_literal: true
# rubocop:disable all

NoSE::Model.new do
  # Define entities along with the size and cardinality of their fields
  # as well as an estimated number of each entity

  (Entity 'categories' do
    ID     'id'
    String 'name', 220
    Integer 'dummy', count: 1
  end) * 500

  (Entity 'regions' do
    ID      'id'
    String  'name', 25, count: 49
    Integer 'dummy', count: 1
  end) * 50

  (Entity 'users' do
    ID         'id'
    String     'firstname', 6, count: 3007
    String     'lastname', 7, count: 474
    String     'nickname', 12, count: 200_000
    String     'password', 15, count: 200_000
    String     'email', 23, count: 197403
    Integer    'rating', count: 251
    Float      'balance', count: 86627
    Date       'creation_date', count: 199346
  end) * 200_000

  (Entity 'items' do
    ID         'id'
    String     'name', 19, count: 250
    String     'description', 197, count: 1999987
    Float      'initial_price', count: 100001
    Integer    'quantity', count: 11
    Float      'reserve_price', count: 100001
    Float      'buy_now', count: 100001
    Integer    'nb_of_bids', count: 101
    Float      'max_bid', count: 100001
    Date       'start_date', count: 1937906
    Date       'end_date', count: 1937880
  end) * 2_000_000

  (Entity 'bids' do
    ID         'id'
    Integer    'qty', count: 5
    Float      'bid', count: 100001
    Date       'date', count: 14812716
  end) * 200_000_000

  (Entity 'comments' do
    ID         'id'
    Integer    'rating', count: 11
    Date       'date', count: 8571118
    String     'comment', 130, count: 9993689
  end) * 10_000_000

  (Entity 'buynow' do
    ID         'id'
    Integer    'qty', count: 3
    Date       'date', count: 1937841
  end) * 2000_000

  HasOne 'region',       'users',
         'users'      => 'regions'

  HasOne 'seller',       'items_sold',
         'items'      => 'users'

  HasOne 'category',     'items',
         'items'      => 'categories'

  HasOne 'user',         'bids',
         'bids'       => 'users'

  HasOne 'item',         'bids',
         'bids'       => 'items'

  HasOne 'from_user',    'comments_sent',
         'comments'   => 'users'

  HasOne 'to_user',      'comments_received',
         'comments'   => 'users'

  HasOne 'item',         'comments',
         'comments'   => 'items'

  HasOne 'buyer',        'bought_now',
         'buynow'     => 'users'

  HasOne 'item',         'bought_now',
         'buynow'     => 'items'
end

# rubocop:enable all
